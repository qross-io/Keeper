package io.qross.model

import io.qross.util._
import scala.sys.process._
import io.qross.model.QrossTask.DataHubExt

object QrossAction {

    //TaskStarter - execute()
    def getTaskCommandsToExecute(taskId: Long, status: String): DataTable = synchronized {
        val ds = new DataSource()

        val job = ds.executeDataRow(s"SELECT id AS job_id, concurrent_limit FROM qross_jobs WHERE id=(SELECT job_id FROM qross_tasks WHERE id=$taskId)")
        val jobId = job.getInt("job_id")

        if (status == TaskStatus.READY) {
            //job enabled = true
            //get job id
            //get job concurrent_limit by job id
            //get concurrent task count by job id
            //update tasks if meet the condition - concurrent_limit=0 OR concurrent < concurrent_limit

            //no commands -> restart whole task - regenerate DAG
            //all done -> finished
            //exceptional exists -> failed -> restart task
            //waiting exists -> running

            val concurrentLimit = job.getInt("concurrent_limit")
            if (concurrentLimit == 0 || ds.executeDataRow(s"SELECT COUNT(0) AS concurrent FROM qross_tasks WHERE job_id=$jobId AND status='${TaskStatus.EXECUTING}'").getInt("concurrent") < concurrentLimit) {
                val map = ds.executeHashMap(s"SELECT status, COUNT(0) AS amount FROM qross_tasks_dags WHERE task_id=$taskId GROUP BY status")
                if (map.isEmpty) {
                    //quit if no commands to execute
                    ds.executeNonQuery(s"UPDATE qross_tasks SET status='${TaskStatus.NO_COMMANDS}' WHERE id=$taskId")
                }
                else if (map.contains(ActionStatus.EXCEPTIONAL) || map.contains(ActionStatus.OVERTIME)) {
                    //restart task if exceptional or overtime
                    ds.executeNonQuery(s"UPDATE qross_tasks SET status='${TaskStatus.RESTARTING}', start_time=NULL, finish_time=NULL, spent=NULL WHERE id=$taskId")
                    ds.executeNonQuery(s"INSERT INTO qross_message_box (message_type, message_key, message_text) VALUES ('TASK', 'RESTART', '${if (map.isEmpty) "WHOLE" else "^EXCEPTIONAL"}@$taskId')")
                    TaskRecord.of(jobId, taskId).warn(s"Task $taskId of job $jobId restart because of exception on ready.")
                }
                else if (map.contains(ActionStatus.WAITING) || map.contains(ActionStatus.QUEUING) || map.contains(ActionStatus.RUNNING)) {
                    //executing
                    ds.executeNonQuery(s"UPDATE qross_tasks SET status='${TaskStatus.EXECUTING}', start_time=NOW() WHERE id=$taskId")
                    TaskRecord.of(jobId, taskId).log(s"Task $taskId of job $jobId start executing on ready.")
                }
                else {
                    //finished
                    ds.executeNonQuery(s"UPDATE qross_tasks SET status='${TaskStatus.FINISHED}' WHERE id=$taskId")
                    TaskRecord.of(jobId, taskId).warn(s"Task $taskId of job $jobId changes status to '${TaskStatus.FINISHED}' because of no commands to be execute on ready.")
                }
            }
            else {
                TaskRecord.of(jobId, taskId).warn(s"Concurrent reach upper limit of Job $jobId for Task $taskId on ready.")
            }
        }

        val executable = ds.executeDataTable(
            s"""SELECT A.action_id, A.job_id, A.task_id, A.command_id, B.task_time, C.command_type, A.command_text, C.overtime, C.retry_limit, D.title, D.owner
                         FROM (SELECT id AS action_id, job_id, task_id, command_id, command_text FROM qross_tasks_dags WHERE task_id=$taskId AND status='${ActionStatus.WAITING}' AND upstream_ids='') A
                         INNER JOIN (SELECT id, task_time FROM qross_tasks WHERE id=$taskId AND status='${TaskStatus.EXECUTING}') B ON A.task_id=B.id
                         INNER JOIN (SELECT id, command_type, overtime, retry_limit FROM qross_jobs_dags WHERE job_id=$jobId) C ON A.command_id=C.id
                         INNER JOIN (SELECT id, title, owner FROM qross_jobs WHERE id=$jobId) D ON A.job_id=D.id""")

        //prepare to run command - start time point
        ds.tableUpdate(s"UPDATE qross_tasks_dags SET start_time=NOW(), status='${ActionStatus.QUEUING}' WHERE id=#action_id", executable)

        ds.close()

        executable
    }

    //TaskExecutor
    def executeTaskCommand(taskCommand: DataRow): Long = {

        val dh = new DataHub()

        val jobId = taskCommand.getInt("job_id")
        val taskId = taskCommand.getLong("task_id")
        val commandId = taskCommand.getInt("command_id")
        val actionId = taskCommand.getLong("action_id")
        val taskTime = taskCommand.getString("task_time")
        val retryLimit = taskCommand.getInt("retry_limit")
        val overtime = taskCommand.getInt("overtime")

        var commandText = taskCommand.getString("command_text")
        commandText = commandText.replace("${jobId}", s"$jobId")
        commandText = commandText.replace("${taskId}", s"$taskId")
        commandText = commandText.replace("${taskTime}", taskTime)
        commandText = commandText.replace("${commandId}", s"$commandId")
        commandText = commandText.replace("${actionId}", s"$actionId")
        commandText = commandText.replace("%QROSS_VERSION", Global.QROSS_VERSION)
        commandText = commandText.replace("%JAVA_BIN_HOME", Global.JAVA_BIN_HOME)
        commandText = commandText.replace("%QROSS_HOME", Global.QROSS_HOME)

        //replace datetime format
        while (commandText.contains("${") && commandText.contains("}")) {
            val ahead = commandText.substring(0, commandText.indexOf("${"))
            var format = commandText.substring(commandText.indexOf("${") + 2)
            val latter = format.substring(format.indexOf("}") + 1)
            format = format.substring(0, format.indexOf("}"))
            commandText = ahead + DateTime(taskTime).sharp(format) + latter
        }

        dh.set(s"UPDATE qross_tasks_dags SET status='${ActionStatus.RUNNING}', run_time=NOW(), waiting=TIMESTAMPDIFF(SECOND, start_time, NOW()) WHERE id=$actionId")
            .set(s"DELETE FROM qross_tasks_logs WHERE task_id=$taskId AND command_id=$commandId") //clear logs of old actions too

        var retry = -1
        var exitValue = 1
        var next = false

        //LET's GO!
        val logger = TaskRecord.of(jobId, taskId).run(commandId, actionId)
        logger.debug(s"START action $actionId - command $commandId of task $taskId - job $jobId: $commandText")

        do {
            if (retry > 0) logger.debug(s"Action $actionId - command $commandId of task $taskId - job $jobId: retry $retry of limit $retryLimit")
            val start = System.currentTimeMillis()
            var timeout = false

            try {
                val process = commandText.run(ProcessLogger(out => {
                    logger.out(out)
                }, err => {
                    logger.err(err)
                }))

                while (process.isAlive()) {
                    //if timeout
                    if (overtime > 0 && (System.currentTimeMillis() - start) / 1000 > overtime) {
                        process.destroy() //kill it
                        timeout = true

                        logger.warn(s"Action $actionId - command $commandId of task $taskId - job $jobId is TIMEOUT: $commandText")
                    }

                    Timer.sleep(1)
                }

                exitValue = process.exitValue()
            }
            catch {
                case e: Exception =>
                    e.printStackTrace()
                    logger.warn(s"Action $actionId - command $commandId of task $taskId - job $jobId is exceptional: ${e.getMessage}")

                    exitValue = 2
            }

            if (timeout) exitValue = -1

            retry += 1
        }
        while (retry < retryLimit && exitValue != 0)

        logger.debug(s"FINISH action $actionId - command $commandId of task $taskId - job $jobId with exitValue $exitValue and status ${if (exitValue == 0) "SUCCESS" else if (exitValue > 0)  "FAILURE" else "TIMEOUT/INTERRUPTED" }")

        exitValue match {
            //finished
            case 0 =>
                //update DAG status
                dh.set(s"UPDATE qross_tasks_dags SET status='${ActionStatus.DONE}', elapsed=TIMESTAMPDIFF(SECOND, run_time, NOW()), finish_time=NOW(), retry_times=$retry WHERE id=$actionId")
                //update DAG dependencies
                dh.set(s"UPDATE qross_tasks_dags SET upstream_ids=REPLACE(upstream_ids, '($commandId)', '') WHERE task_id=$taskId AND status='${ActionStatus.WAITING}' AND POSITION('($commandId)' IN upstream_ids)>0")

                //if continue
                next = dh.executeExists(s"SELECT id FROM qross_tasks_dags WHERE task_id=$taskId AND status='${ActionStatus.WAITING}' LIMIT 1")
                if (!next) {
                    //meet: no waiting action, no running action
                    //action status: all done - task status: executing -> finished
                    //if exceptional action exists - task status: executing, finished -> failed

                    //update task status if all finished
                    dh.set(s"UPDATE qross_tasks SET finish_time=NOW(), spent=TIMESTAMPDIFF(SECOND, start_time, NOW()), status='${TaskStatus.FINISHED}', checked='' WHERE id=$taskId AND NOT EXISTS (SELECT id FROM qross_tasks_dags WHERE task_id=$taskId AND status!='${ActionStatus.DONE}')")
                    
                    //check "after" dependencies
                    dh.get(s"SELECT A.id, A.task_id, A.dependency_type, A.dependency_value, A.ready, B.task_time FROM qross_tasks_dependencies A INNER JOIN qross_tasks B ON A.job_id=B.job_id WHERE B.status='${TaskStatus.FINISHED}' AND A.task_id=$taskId AND A.dependency_moment='after' AND A.ready='no'")
                            .foreach(row => {
                                val result = TaskDependency.check(row.getString("dependency_type"), row.getString("dependency_value"))
                                row.set("ready", result._1)
                                row.set("dependency_value", result._2)
                            }).put("UPDATE qross_tasks_dependencies SET ready='#ready', dependency_value='#dependency_value' WHERE id=#id")

                    //dh.set(s"UPDATE qross_tasks SET status='${TaskStatus.INCORRECT}', checked='no' WHERE id=$taskId AND status='${TaskStatus.FINISHED}' AND EXISTS(SELECT id FROM qross_tasks_dependencies WHERE task_id=$taskId AND dependency_moment='after' AND ready='no')")
                    dh.set(s"UPDATE qross_tasks A INNER JOIN (SELECT task_id, COUNT(0) AS amount FROM qross_tasks_dependencies WHERE task_id=$taskId AND dependency_moment='after' AND ready='no') B ON A.id=B.task_id AND A.id=$taskId AND A.status='${TaskStatus.FINISHED}' SET status=IF(B.amount > 0, '${TaskStatus.INCORRECT}', '${TaskStatus.SUCCESS}'), checked=IF(B.amount > 0, 'no', '')")
                }
            //timeout
            case -1 =>
                dh.set(s"UPDATE qross_tasks_dags SET status='${ActionStatus.OVERTIME}', retry_times=$retry WHERE id=$actionId")
                dh.set(s"UPDATE qross_tasks SET status='${TaskStatus.TIMEOUT}', checked='no' WHERE id=$taskId")
            //failed
            case _ =>
                dh.set(s"UPDATE qross_tasks_dags SET status='${ActionStatus.EXCEPTIONAL}', retry_times=$retry WHERE id=$actionId")
                dh.set(s"UPDATE qross_tasks SET finish_time=NOW(), status='${TaskStatus.FAILED}', checked='no' WHERE id=$taskId")
        }
    
        val status = dh.executeSingleValue(s"SELECT status FROM qross_tasks WHERE id=$taskId").getOrElse("miss")
        //send notification mail if failed or timeout or incorrect
        if (status == TaskStatus.SUCCESS || status == TaskStatus.FAILED || status == TaskStatus.TIMEOUT || status == TaskStatus.INCORRECT) {
    
            dh.get(s"SELECT job_id, event_function, event_value FROM qross_jobs_events WHERE job_id=$jobId AND enabled='yes' AND event_name='onTask${status.capitalize}'")
            if (dh.nonEmpty) {

                dh.cache("events")
                dh.cache("task", DataTable(taskCommand))

                dh.get(s"SELECT CAST(create_time AS CHAR) AS create_time, log_type, log_text FROM qross_tasks_logs WHERE task_id=$taskId AND action_id=$actionId ORDER BY create_time ASC")
                    .buffer("logs")

                dh.openCache()
                    .get("SELECT A.*, B.event_value AS receivers FROM task A INNER JOIN events B ON A.job_id=B.job_id WHERE event_function='SEND_MAIL_TO'")
                        .sendEmailWithLogs(status)
                    .get("SELECT * FROM task WHERE EXISTS (SELECT job_id FROM events WHERE event_function='REQUEST_API')")
                        .requestApi(status)
                    .get("SELECT event_value, '' AS restart_time FROM events WHERE INSTR(event_function, 'RESTART_')=1")
                        .foreach(row => {
                            row.set("restart_time", DateTime.now.plusMinutes(row.getInt("event_value", 30)).getString("yyyyMMddHHmm00"))
                        }).put(s"UPDATE qross_tasks SET restart_time='#restart_time' WHERE id=$taskId")
            }
        }

        dh.close()

        if (next) {
            //return
            taskId
        } else {
            TaskRecord.of(jobId, taskId).debug(s"Task $taskId of job $jobId finish with status ${status.toUpperCase}.")
            //clear logger
            TaskRecord.dispose(taskId)
            //return
            0
        }
    }
}
