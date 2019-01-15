package io.qross.model;

object QrossAction {

    //TaskStarter - execute()
    def getTaskCommandsToExecute(task: Task): DataTable = synchronized {

        val ds = new DataSource()

        val taskId = task.id
        val jobId = task.jobId
        val status = task.status
        val recordTime = task.recordTime
        val concurrentLimit = ds.executeSingleValue(s"SELECT concurrent_limit FROM qross_jobs WHERE id=$jobId").getOrElse("1").toInt

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

            if (concurrentLimit == 0 || ds.executeDataRow(s"SELECT COUNT(0) AS concurrent FROM qross_tasks WHERE job_id=$jobId AND status='${TaskStatus.EXECUTING}'").getInt("concurrent") < concurrentLimit) {
                val map = ds.executeHashMap(s"SELECT status, COUNT(0) AS amount FROM qross_tasks_dags WHERE task_id=$taskId AND record_time='$recordTime' GROUP BY status")
                if (map.isEmpty) {
                    //quit if no commands to execute
                    ds.executeNonQuery(s"UPDATE qross_tasks SET status='${TaskStatus.NO_COMMANDS}' WHERE id=$taskId")
                    if (recordTime == "") println("@X 1")
                    TaskRecorder.of(jobId, taskId, recordTime).warn(s"Task $taskId of job $jobId at <$recordTime> has been  closed because no commands exists on task ready.")
                }
                else if (map.contains(ActionStatus.EXCEPTIONAL) || map.contains(ActionStatus.OVERTIME)) {
                    //restart task if exceptional or overtime
                    ds.executeNonQuery(s"INSERT INTO qross_message_box (message_type, message_key, message_text) VALUES ('TASK', 'RESTART', '^EXCEPTIONAL@$taskId')")
                    if (recordTime == "") println("@X 2")
                    TaskRecorder.of(jobId, taskId, recordTime).warn(s"Task $taskId of job $jobId at <$recordTime> restart because EXCEPTIONAL commands exists on task ready.")
                }
                else if (map.contains(ActionStatus.WAITING) || map.contains(ActionStatus.QUEUEING) || map.contains(ActionStatus.RUNNING)) {
                    //executing
                    ds.executeNonQuery(s"UPDATE qross_tasks SET status='${TaskStatus.EXECUTING}', start_time=NOW() WHERE id=$taskId")
                    if (recordTime == "") println("@X 3")
                    TaskRecorder.of(jobId, taskId, recordTime).log(s"Task $taskId of job $jobId at <$recordTime> start executing on task ready.")
                }
                else {
                    //finished
                    ds.executeNonQuery(s"UPDATE qross_tasks SET status='${TaskStatus.FINISHED}' WHERE id=$taskId")
                    if (recordTime == "") println("@X 4")
                    TaskRecorder.of(jobId, taskId, recordTime).warn(s"Task $taskId of job $jobId at <$recordTime> changes status to '${TaskStatus.FINISHED}' because all commands has been executed on task ready.")
                }
            }
            else {
                if (recordTime == "") println("@X 5")
                TaskRecorder.of(jobId, taskId, recordTime).warn(s"Concurrent reach upper limit of Job $jobId for Task $taskId at <$recordTime> on task ready.")
            }
        }

        val executable = ds.executeDataTable(
            s"""SELECT A.action_id, A.job_id, A.task_id, A.command_id, B.task_time, B.record_time, C.command_type, A.command_text, C.overtime, C.retry_limit, D.title, D.owner
                         FROM (SELECT id AS action_id, job_id, task_id, command_id, command_text FROM qross_tasks_dags WHERE task_id=$taskId AND record_time='${recordTime}' AND status='${ActionStatus.WAITING}' AND upstream_ids='') A
                         INNER JOIN (SELECT id, task_time, record_time FROM qross_tasks WHERE id=$taskId AND status='${TaskStatus.EXECUTING}') B ON A.task_id=B.id
                         INNER JOIN (SELECT id, command_type, overtime, retry_limit FROM qross_jobs_dags WHERE job_id=$jobId) C ON A.command_id=C.id
                         INNER JOIN (SELECT id, title, owner FROM qross_jobs WHERE id=$jobId) D ON A.job_id=D.id""")

        //prepare to run command - start time point
        ds.tableUpdate(s"UPDATE qross_tasks_dags SET start_time=NOW(), status='${ActionStatus.QUEUEING}' WHERE id=#action_id", executable)

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
        val recordTime = taskCommand.getString("record_time")

        var commandText = taskCommand.getString("command_text")
        commandText = commandText.replace("${jobId}", s"$jobId")
        commandText = commandText.replace("${taskId}", s"$taskId")
        commandText = commandText.replace("${taskTime}", taskTime)
        commandText = commandText.replace("${commandId}", s"$commandId")
        commandText = commandText.replace("${actionId}", s"$actionId")
        commandText = commandText.replace("%QROSS_VERSION", Global.QROSS_VERSION)
        commandText = commandText.replace("%JAVA_BIN_HOME", Global.JAVA_BIN_HOME)
        commandText = commandText.replace("%QROSS_HOME", Global.QROSS_HOME)

        //auto add environment variable to ahead
        if (commandText.startsWith("java ")) {
            commandText = Global.JAVA_BIN_HOME + commandText
        }
        else if (commandText.startsWith("python2 ")) {
            commandText = Global.PYTHON2_HOME + commandText
        }
        else if (commandText.startsWith("python3 ")) {
            commandText = Global.PYTHON3_HOME + commandText
        }

        //replace datetime format
        while (commandText.contains("${") && commandText.contains("}")) {
            val ahead = commandText.substring(0, commandText.indexOf("${"))
            var format = commandText.substring(commandText.indexOf("${") + 2)
            val latter = format.substring(format.indexOf("}") + 1)
            format = format.substring(0, format.indexOf("}"))
            commandText = ahead + DateTime(taskTime).sharp(format) + latter
        }

        dh.set(s"UPDATE qross_tasks_dags SET status='${ActionStatus.RUNNING}', run_time=NOW(), waiting=TIMESTAMPDIFF(SECOND, start_time, NOW()) WHERE id=$actionId")
            //.set(s"DELETE FROM qross_tasks_logs WHERE task_id=$taskId AND command_id=$commandId") //clear logs of old actions too

        var retry = -1
        var exitValue = 1
        var next = false

        //LET's GO!
        val logger = TaskRecorder.of(jobId, taskId, recordTime).run(commandId, actionId)
        if (recordTime == "") println("@X 6")
        logger.debug(s"START action $actionId - command $commandId of task $taskId - job $jobId at <$recordTime>: $commandText")

        do {
            if (retry > 0) {
                if (recordTime == "") println("@X 7")
                logger.debug(s"Action $actionId - command $commandId of task $taskId - job $jobId at <$recordTime>: retry $retry of limit $retryLimit")
            }
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
                        if (recordTime == "") println("@X A")
                        logger.warn(s"Action $actionId - command $commandId of task $taskId - job $jobId at <$recordTime> is TIMEOUT: $commandText")
                    }

                    Timer.sleep(1)
                }

                exitValue = process.exitValue()
            }
            catch {
                case e: Exception =>
                    e.printStackTrace()

                    val buf = new java.io.ByteArrayOutputStream()
                    e.printStackTrace(new java.io.PrintWriter(buf, true))
                    logger.err(buf.toString())
                    buf.close()

                    logger.err(s"Action $actionId - command $commandId of task $taskId - job $jobId is exceptional: ${e.getMessage}")

                    exitValue = 2
            }

            if (timeout) exitValue = -1

            retry += 1
        }
        while (retry < retryLimit && exitValue != 0)

        if (recordTime == "") println("@X 8")
        logger.debug(s"FINISH action $actionId - command $commandId of task $taskId - job $jobId with exitValue $exitValue and status ${if (exitValue == 0) "SUCCESS" else if (exitValue > 0)  "FAILURE" else "TIMEOUT/INTERRUPTED" }")

        exitValue match {
            //finished
            case 0 =>
                //update DAG status
                dh.set(s"UPDATE qross_tasks_dags SET status='${ActionStatus.DONE}', elapsed=TIMESTAMPDIFF(SECOND, run_time, NOW()), finish_time=NOW(), retry_times=$retry WHERE id=$actionId")
                //update DAG dependencies
                dh.set(s"UPDATE qross_tasks_dags SET upstream_ids=REPLACE(upstream_ids, '($commandId)', '') WHERE task_id=$taskId AND record_time='$recordTime' AND status='${ActionStatus.WAITING}' AND POSITION('($commandId)' IN upstream_ids)>0")

                //if continue
                next = dh.executeExists(s"SELECT id FROM qross_tasks_dags WHERE task_id=$taskId AND record_time='$recordTime' AND status='${ActionStatus.WAITING}' LIMIT 1")
                if (!next) {
                    //meet: no waiting action, no running action
                    //action status: all done - task status: executing -> finished
                    //if exceptional action exists - task status: executing, finished -> failed

                    //update task status if all finished
                    dh.set(s"UPDATE qross_tasks SET finish_time=NOW(), spent=TIMESTAMPDIFF(SECOND, start_time, NOW()), status='${TaskStatus.FINISHED}', checked='' WHERE id=$taskId AND NOT EXISTS (SELECT id FROM qross_tasks_dags WHERE task_id=$taskId AND record_time='$recordTime' AND status!='${ActionStatus.DONE}')")

                    //check "after" dependencies
                    dh.get(s"SELECT A.id, A.task_id, A.dependency_type, A.dependency_value, A.ready, B.task_time FROM qross_tasks_dependencies A INNER JOIN qross_tasks B ON A.job_id=B.job_id WHERE B.status='${TaskStatus.FINISHED}' AND A.task_id=$taskId AND A.record_time='$recordTime' AND A.dependency_moment='after' AND A.ready='no'")
                        .foreach(row => {
                            val result = TaskDependency.check(row.getString("dependency_type"), row.getString("dependency_value"), taskId, recordTime)
                            row.set("ready", result._1)
                            row.set("dependency_value", result._2)
                        }).put("UPDATE qross_tasks_dependencies SET ready='#ready', dependency_value='#dependency_value' WHERE id=#id")

                    dh.set(s"UPDATE qross_tasks A INNER JOIN (SELECT task_id, COUNT(0) AS amount FROM qross_tasks_dependencies WHERE task_id=$taskId AND record_time='$recordTime' AND dependency_moment='after' AND ready='no') B ON A.id=B.task_id AND A.id=$taskId AND A.status='${TaskStatus.FINISHED}' SET status=IF(B.amount > 0, '${TaskStatus.INCORRECT}', '${TaskStatus.SUCCESS}'), checked=IF(B.amount > 0, 'no', '')")
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

            dh.get(s"SELECT job_id, event_function, event_value, event_option FROM qross_jobs_events WHERE job_id=$jobId AND enabled='yes' AND event_name='onTask${status.capitalize}'")
            if (dh.nonEmpty) {

                dh.cache("events")
                dh.cache("task", DataTable(taskCommand))

                dh.get(s"SELECT CAST(create_time AS CHAR) AS create_time, log_type, log_text FROM qross_tasks_logs WHERE task_id=$taskId AND action_id=$actionId ORDER BY create_time ASC")
                    .buffer("logs")

                dh.openCache()
                    .get("SELECT A.*, B.event_value AS receivers FROM task A INNER JOIN events B ON A.job_id=B.job_id AND event_function='SEND_MAIL_TO'")
                        .sendEmailWithLogs(status)
                    .get("SELECT A.*, B.event_value AS api FROM task A INNER events B ON A.job_id=B.job_id AND B.event_function='REQUEST_API'")
                        .requestApi(status)
                    .get(s"SELECT event_value, '' AS to_be_start_time FROM events WHERE INSTR(event_function, 'RESTART_')=1 AND <=event_option") //SIGNED is MySQL syntax, but SQLite will ignore it.
                        .restartTask(jobId, taskId, recordTime)
            }
        }

        dh.close()

        if (next) {
            //return
            taskId
        } else {
            //record and clear logger
            if (recordTime == "") println("@X D")
            TaskRecorder.of(jobId, taskId, recordTime).debug(s"Task $taskId of job $jobId at <$recordTime> finish with status ${status.toUpperCase}.").dispose()
            //return nothing
            0
        }
    }
}
