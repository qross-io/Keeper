package io.qross.model

import io.qross.util._

import scala.sys.process.ProcessLogger

object QrossAction {

    //TaskStarter - execute()
    def getTaskCommandsToExecute(taskId: Long, status: String): DataTable = synchronized {
        val ds = new DataSource()

        //get
        if (status == TaskStatus.READY) {
            //job enabled = true
            //get job id
            //get job concurrent_limit by job id
            //get concurrent task count by job id
            //update tasks if meet the condition - concurrent_limit=0 OR concurrent < concurrent_limit

            //no commands -> miss_commands -> SELECT id FROM qross_tasks_dags WHERE task_id=$taskId LIMIT 1
            //all done -> finished -> SELECT id FROM qross_tasks_dags WHERE task_id=$taskId AND status<>'done'
            //exceptional exists -> failed -> SELECT id FROM qross_tasks_dags WHERE task_id=$taskId AND status='exceptional' LIMIT 1
            //waiting exists -> running -> SELECT id FROM qross_tasks_dags WHERE task_id=$taskId AND status='running'

            val job = ds.executeDataRow(s"SELECT id AS job_id, concurrent_limit, enabled FROM qross_jobs WHERE id=(SELECT job_id FROM qross_tasks WHERE id=$taskId)")
            if (job.getBoolean("enabled")) {
                val concurrentLimit = job.getInt("concurrent_limit")
                val jobId = job.getInt("job_id")
                if (concurrentLimit == 0 || ds.executeDataRow(s"SELECT COUNT(0) AS concurrent FROM qross_tasks WHERE job_id=$jobId AND status='executing'").getInt("concurrent") < concurrentLimit) {
                    val map = ds.executeHashMap(s"SELECT status, COUNT(0) AS amount FROM qross_tasks_dags WHERE task_id=$taskId GROUP BY status")
                    if (map.isEmpty || map.contains("exceptional") || map.contains("overtime")) {
                        //restart task
                        ds.executeNonQuery(s"UPDATE qross_tasks SET status='restarting',start_time=NULL,finish_time=NULL,spent=NULL WHERE id=$taskId;")
                        ds.executeNonQuery(s"INSERT INTO qross_message_box (message_type, message_key, message_text) VALUES ('TASK', 'RESTART', '${if (map.isEmpty) "WHOLE" else "^EXCEPTIONAL"}@$taskId')")
                        TaskRecord.of(jobId, taskId).warn(s"Task $taskId of job $jobId restart because of exception on ready.")
                    }
                    else if (map.contains("waiting") || map.contains("queuing")) {
                        //executing
                        ds.executeNonQuery(s"UPDATE qross_tasks SET status='executing', start_time=NOW() WHERE id=$taskId")
                        TaskRecord.of(jobId, taskId).log(s"Task $taskId of job $jobId start executing on ready.")
                    }
                    else {
                        //finished
                        ds.executeNonQuery(s"UPDATE qross_tasks SET status='finished' WHERE id=$taskId")
                        TaskRecord.of(jobId, taskId).warn(s"Task $taskId of job $jobId changes status to 'finished' because of no commands to be execute on ready.")
                    }
                }
                else {
                    TaskRecord.of(jobId, taskId).warn(s"Concurrent reach upper limit of Job $jobId for Task $taskId on ready.")
                }
            }
        }

        val executable = ds.executeDataTable(
            s"""SELECT A.action_id, A.job_id, A.task_id, A.command_id, B.task_time, C.command_type, C.command_text,
                         C.overtime, D.title, D.owner, D.mail_notification, D.mail_master_on_exception, D.retry_delay_on_exception, C.retry_limit
                         FROM (SELECT id AS action_id, job_id, task_id, command_id FROM qross_tasks_dags WHERE task_id=$taskId AND status='waiting' AND upstream_ids='') A
                         INNER JOIN (SELECT id, task_time FROM qross_tasks WHERE id=$taskId AND status='executing') B ON A.task_id=B.id
                         INNER JOIN qross_jobs_dags C ON A.command_id=C.id
                         INNER JOIN qross_jobs D ON A.job_id=D.id""")

        //prepare to run command - start time point
        ds.tableUpdate("UPDATE qross_tasks_dags SET start_time=NOW(), status='queuing' WHERE id=#action_id", executable)

        ds.close()

        executable
    }

    //TaskExecutor
    def executeTaskCommand(taskCommand: DataRow): Long = {

        val dh = new DataHub()

        val actionId = taskCommand.getLong("action_id")
        val jobId = taskCommand.getInt("job_id")
        val taskId = taskCommand.getLong("task_id")
        val taskTime = taskCommand.getString("task_time")
        val commandId = taskCommand.getInt("command_id")
        val retryLimit = taskCommand.getInt("retry_limit")
        val overtime = taskCommand.getInt("overtime")
        val owner = taskCommand.getString("owner")
        val retryDelay = taskCommand.getInt("retry_delay_on_exception")

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
            commandText = ahead + DateTime(taskTime).sharp(format).mkString(",") + latter
        }

        dh.executeNonQuery(s"UPDATE qross_tasks_dags SET status='running', run_time=NOW(), waiting=TIMESTAMPDIFF(SECOND, start_time, NOW()) WHERE id=$actionId")
        dh.executeNonQuery(s"DELETE FROM qross_tasks_logs WHERE task_id=$taskId AND command_id=$commandId") //clear logs of old actions too

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
            if (timeout) exitValue = -1

            retry += 1
        }
        while (retry < retryLimit && exitValue != 0)

        logger.debug(s"FINISH action $actionId - command $commandId of task $taskId - job $jobId with exitValue $exitValue and status ${if (exitValue == 0) "SUCCESS" else if (exitValue > 0)  "FAILURE" else "TIMEOUT/INTERRUPTED" }")

        exitValue match {
            //finished
            case 0 =>
                //update DAG status
                dh.executeNonQuery(s"UPDATE qross_tasks_dags SET status='done', elapsed=TIMESTAMPDIFF(SECOND, run_time, NOW()), finish_time=NOW(), retry_times=$retry WHERE id=$actionId")
                //update DAG dependencies
                dh.executeNonQuery(s"UPDATE qross_tasks_dags SET upstream_ids=REPLACE(upstream_ids, '($commandId)', '') WHERE task_id=$taskId AND status='waiting' AND POSITION('($commandId)' IN upstream_ids)>0;")

                //if continue
                next = dh.executeExists(s"SELECT id FROM qross_tasks_dags WHERE task_id=$taskId AND status='waiting' LIMIT 1")
                if (!next) {
                    //meet: no waiting action, no running action
                    //action status: all done - task status: executing -> finished
                    //if exceptional action exists - task status: executing, finished -> failed

                    //update task status if all finished
                    dh.executeNonQuery(s"UPDATE qross_tasks SET finish_time=NOW(), spent=TIMESTAMPDIFF(SECOND, start_time, NOW()), status='finished', checked='' WHERE id=$taskId AND NOT EXISTS (SELECT id FROM qross_tasks_dags WHERE task_id=$taskId AND status!='done')")
                    //dh.executeNonQuery(s"UPDATE qross_tasks SET finish_time=NOW(), status='failed', checked='no' WHERE id=$taskId AND status IN ('executing', 'finished') AND NOT EXISTS (SELECT id FROM qross_tasks_dags WHERE task_id=$taskId AND status='running') AND EXISTS(SELECT id FROM qross_tasks_dags WHERE task_id=$taskId AND status='exceptional')")  //6.12

                    //check "after" dependencies
                    dh.get(s"SELECT A.id, A.task_id, A.dependency_type, A.dependency_value, A.ready, B.task_time FROM qross_tasks_dependencies A INNER JOIN qross_tasks B ON A.job_id=B.job_id WHERE B.status='finished' AND A.task_id=$taskId AND A.dependency_moment='after' AND A.ready='no'")
                            .foreach(row => {
                                val result = TaskDependency.check(row.getString("dependency_type"), row.getString("dependency_value"))
                                row.set("ready", result._1)
                                row.set("dependency_value", result._2)
                            }).put("UPDATE qross_tasks_dependencies SET ready=$ready, dependency_value=$dependency_value WHERE id=$id")

                    //update tasks status if incorrect
                    dh.executeNonQuery(s"UPDATE qross_tasks SET status='incorrect', checked='no' WHERE id=$taskId AND status='finished' AND EXISTS(SELECT id FROM qross_tasks_dependencies WHERE task_id=$taskId AND dependency_moment='after' AND ready='no')")
                }
            //timeout
            case -1 =>
                dh.executeNonQuery(s"UPDATE qross_tasks_dags SET status='timeover', retry_times=$retry WHERE id=$actionId")
                dh.executeNonQuery(s"UPDATE qross_tasks SET status='timeout', checked='no' WHERE id=$taskId")
            //failed
            case _ =>
                dh.executeNonQuery(s"UPDATE qross_tasks_dags SET status='exceptional', retry_times=$retry WHERE id=$actionId")
                dh.executeNonQuery(s"UPDATE qross_tasks SET finish_time=NOW(), status='failed', checked='no' WHERE id=$taskId")
        }

        val status = dh.executeSingleValue(s"SELECT status FROM qross_tasks WHERE id=$taskId").getOrElse("miss")
        //send notification mail if failed or timeout or incorrect
        if (status == "failed" || status == "timeout" || status == "incorrect") {
            if (Global.EMAIL_NOTIFICATION && taskCommand.getBoolean("mail_notification") && owner != "") {
                OpenResourceFile("/templates/failed_incorrect.html")
                        .replace("${status}", status.toUpperCase())
                        .replaceWith(taskCommand)
                        .replace("${logs}", TaskRecord.toHTML(dh.executeDataTable(s"SELECT CAST(create_time AS CHAR) AS create_time, log_type, log_text FROM qross_tasks_logs WHERE task_id=$taskId AND action_id=$actionId ORDER BY create_time ASC")))
                        .writeEmail(s"${status.toUpperCase()}: ${taskCommand.getString("title")} $taskTime - TaskID: $taskId")
                        .to(owner)
                        .cc(if (taskCommand.getBoolean("mail_master_on_exception")) Global.MASTER_USER_GROUP else "")
                        .send()
            }

            //retry delay on exception
            if (retryDelay > 0) {
                dh.executeNonQuery(s"UPDATE qross_tasks SET restart_time=${DateTime.now.plusMinutes(retryDelay).getString("yyyyMMddHHmm00")} WHERE id=$taskId")
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
