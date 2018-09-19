package io.qross.model

import io.qross.util.Output._
import io.qross.util._

import scala.util.{Success, Try}

object QrossTask {

    implicit class DataHubExt(dh: DataHub) {

        def sendEmail(taskStatus: String): DataHub = {
            dh.TABLE.firstRow match {
                case Some(row) =>  TaskEvent.sendMail(taskStatus, row, dh.BUFFER("logs"))
                case None =>
            }
            dh
        }

        def requestApi(taskStatus: String): DataHub = {
            dh.TABLE.firstRow match {
                case Some(row) =>  TaskEvent.requestApi(taskStatus, row)
                case None =>
            }
            dh
        }

        def generateDependencies(): DataHub = {
            if (dh.nonEmpty) {
                val table = DataTable.withFields("job_id" -> DataType.INTEGER,
                                                        "task_id" -> DataType.INTEGER,
                                                        "dependency_id" -> DataType.INTEGER,
                                                        "dependency_moment" -> DataType.TEXT,
                                                        "dependency_type" -> DataType.TEXT,
                                                        "dependency_value" -> DataType.TEXT)

                dh.foreach(row => {
                    TaskDependency.parseDependencyValue(row.getString("job_id"), row.getString("task_id"), row.getString("dependency_value"), row.getString("task_time"))
                            .foreach(value =>
                                table.insertRow("job_id" -> row.getInt("job_id"),
                                    "task_id" -> row.getLong("task_id"),
                                    "dependency_id" -> row.getInt("dependency_id"),
                                    "dependency_moment" -> row.getString("dependency_moment"),
                                    "dependency_type" -> row.getString("dependency_type"),
                                    "dependency_value" -> value)
                            )
                })

                dh.put("INSERT INTO qross_tasks_dependencies (job_id, task_id, dependency_id, dependency_moment, dependency_type, dependency_value) VALUES (?, ?, ?, ?, ?, ?)", table)
            }

            dh.clear()
        }
    }

    //on keeper start up @ TaskProducer
    def complementTasks(): Unit = {

        /*
        TimeLine:
        last beat -> server offline -> now -> server online -> next beat
        */
        val dh = new DataHub()

        //get last tick of producer
        val lastBeat = (Try(dh.openDefault().executeSingleValue("SELECT UNIX_TIMESTAMP(last_beat_time) FROM qross_keeper_beats WHERE actor_name='TaskProducer'").getOrElse("").toLong) match {
                case Success(tick) => DateTime.of(tick)
                case _ => DateTime.now
            }).setSecond(0).setNano(0).plusMinutes(1)
        val nextBeat = DateTime.now.setSecond(0).setNano(0).plusMinutes(1)

        //get all jobs
        dh.openDefault()
            //next_tick != '' means this is not a new job
            .get(s"SELECT id AS job_id, cron_exp, next_tick, complement_missed_tasks FROM qross_jobs WHERE enabled='yes' AND cron_exp!='' AND next_tick!='' AND next_tick!='NONE' AND next_tick<${nextBeat.getTickValue}")
                .cache("jobs")

        //get all ticks for jobs that need to complement during server was offline
        dh.openCache()
            .get("SELECT job_id, cron_exp, next_tick FROM jobs WHERE complement_missed_tasks='yes'")
                .table("job_id" -> DataType.INTEGER, "next_tick" -> DataType.TEXT) (row => {
                    val table = DataTable()
                    val jobId = row.getInt("job_id")
                    val ticks = try {
                        CronExp.getTicks(row.getString("cron_exp"), lastBeat.getTickValue, nextBeat.getTickValue)
                    }
                    catch {
                        case e: Exception =>
                            Output.writeException(e.getMessage)
                            List[String]()
                    }
                    ticks.foreach(time => {
                        table.insertRow(
                            "job_id" -> jobId,
                            "next_tick" -> time
                        )
                    })
                    table
                }).cache("missed_tasks")

        //get exists tasks during offline
        dh.openDefault()
            .get(s"SELECT job_id, task_time FROM qross_tasks WHERE job_id IN (SELECT id FROM qross_jobs WHERE enabled='yes' AND cron_exp!='' AND next_tick!='' AND next_tick!='NONE' AND complement_missed_tasks='yes') AND task_time>${lastBeat.getTickValue} AND task_time<${nextBeat.getTickValue}")
                .cache("exists_tasks")

        //complement all jobs
        dh.openCache()
            .get(s"SELECT A.job_id, A.next_tick FROM missed_tasks A LEFT JOIN exists_tasks B ON A.job_id=B.job_id AND A.next_tick=B.task_time WHERE B.job_id IS NULL")
                .put("INSERT INTO qross_tasks (job_id, task_time) VALUES (?, ?)")

        //get next tick for all jobs
        dh.openCache()
            .get("SELECT job_id, cron_exp, next_tick FROM jobs")
                .foreach(row => {
                    try {
                        row.set("next_tick", CronExp(row.getString("cron_exp")).getNextTickOrNone(nextBeat))
                    }
                    catch {
                        case e: Exception => Output.writeException(e.getMessage)
                    }
                }).put("UPDATE qross_jobs SET next_tick=$next_tick WHERE id=$job_id")

        //restart executing tasks when Keeper exit exceptionally.
        dh.openDefault()
            .get(s"SELECT A.task_id FROM (SELECT id As task_id, job_id FROM qross_tasks WHERE status='${TaskStatus.EXECUTING}') A INNER JOIN qross_jobs B ON A.job_id=B.id AND B.enabled='yes'")
                .put(s"UPDATE qross_tasks_dags SET status='${ActionStatus.EXCEPTIONAL}' WHERE task_id=#task_id AND status IN ('${ActionStatus.QUEUING}', '${ActionStatus.RUNNING}')")
                .put(s"UPDATE qross_tasks SET status='${TaskStatus.RESTARTING}',start_time=NULL,finish_time=NULL,spent=NULL WHERE id=#task_id")
                .put("INSERT INTO qross_message_box (message_type, message_key, message_text) VALUES ('TASK', 'RESTART', '^EXCEPTIONAL@#task_id')")

        dh.close()
    }

    //TaskProducer
    //create and initialize tasks then return initialized and ready tasks
    def createAndInitializeTasks(tick: String): DataTable = {
        val minute = DateTime(tick)

        val dh = new DataHub()

        //update empty next_tick - it will be empty when create a new job
        //update outdated jobs - it will occur when you enable a job from disabled status
        dh.openDefault()
            .get(s"SELECT id AS job_id, cron_exp, '' AS next_tick FROM qross_jobs WHERE  cron_exp!='' AND (next_tick='' OR (next_tick!='NONE' AND next_tick<$tick))")
                .foreach(row =>
                    try {
                        row.set("next_tick", CronExp(row.getString("cron_exp")).getNextTickOrNone(minute))
                    }
                    catch {
                        case e: Exception => Output.writeException(e.getMessage)
                    }
                ).put("UPDATE qross_jobs SET next_tick=$next_tick WHERE id=$job_id")

        //create tasks without cron_exp
        //excluding jobs with executing tasks
        dh.openDefault()
            .executeNonQuery("INSERT INTO qross_tasks (job_id, task_time) SELECT id, '' FROM qross_jobs WHERE cron_exp='' AND enabled='yes' AND id NOT IN (SELECT job_id FROM qross_tasks WHERE task_time='' AND status NOT IN ('finished', 'incorrect', 'failed', 'timeout'))")

        //jobs with cron_exp
        dh.openDefault()
            .get(s"SELECT id AS job_id, cron_exp, next_tick FROM qross_jobs WHERE next_tick='$tick' AND enabled='yes' AND id NOT IN (SELECT job_id FROM qross_tasks WHERE task_time='$tick')")
                .cache("jobs")
            .get(s"SELECT id AS task_id, job_id, task_time FROM qross_tasks WHERE task_time='$tick'")
                .cache("exists_tasks")

        //create schedule tasks
        dh.openCache()
            .get("SELECT A.job_id, A.next_tick FROM jobs A LEFT JOIN exists_tasks B ON A.job_id=B.job_id AND A.next_tick=B.task_time WHERE B.job_id IS NULL")
                .put("INSERT INTO qross_tasks (job_id, task_time) VALUES (?, ?)")

        //get next tick and update jobs
        minute.plusMinutes(1) //get next minute to match next tick
        dh.openCache()
            .get("SELECT job_id, cron_exp, '' AS next_tick FROM jobs")
                .foreach(row => {
                    try {
                        row.set("next_tick", CronExp(row.getString("cron_exp")).getNextTickOrNone(minute))
                    }
                    catch {
                        case e: Exception => Output.writeException(e.getMessage)
                    }
                }).put("UPDATE qross_jobs SET next_tick=$next_tick WHERE id=$job_id")

        // ----- dependencies -----

        //get all new tasks
        dh.openDefault()
            //.get(s"SELECT A.id AS task_id, A.job_id, A.task_time, A.status, B.dependencies FROM qross_tasks A INNER JOIN qross_jobs B ON A.job_id=B.id AND B.enabled='yes' WHERE A.status='$TaskStatus.NEW'")  // remove qross_jobs.dependencies at 7.2
            .get(
                s"""SELECT A.task_id, A.job_id, A.task_time, IFNULL(B.dependencies, 0) AS dependencies
                   FROM (SELECT id AS task_id, job_id, task_time FROM qross_tasks WHERE status='${TaskStatus.NEW}') A
                   LEFT JOIN (SELECT job_id, COUNT(0) AS dependencies FROM qross_jobs_dependencies WHERE dependency_moment='before' GROUP BY job_id) B ON A.job_id=B.job_id""")
                .cache("tasks")

        //get all dependencies
        dh.openCache()
            .get("SELECT IFNULL(GROUP_CONCAT(DISTINCT job_id), 0) AS job_ids FROM tasks WHERE dependencies>0")
        dh.openDefault()
            .pass("SELECT id AS dependency_id, job_id, dependency_moment, dependency_type, dependency_value FROM qross_jobs_dependencies WHERE job_id IN (#job_ids)")
                .cache("dependencies")

        //generate dependencies
        dh.openCache()
            .get("SELECT A.job_id, A.task_id, A.task_time, B.dependency_id, B.dependency_moment, B.dependency_type, B.dependency_value FROM tasks A INNER JOIN dependencies B ON A.job_id=B.job_id")
                .generateDependencies()

        // ---------- DAGs ----------

        //get all DAGs
        dh.openCache()
            .get("SELECT IFNULL(GROUP_CONCAT(DISTINCT job_id), 0) AS job_ids FROM tasks")
        dh.openDefault()
            .pass("SELECT id AS command_id, command_text, job_id, upstream_ids FROM qross_jobs_dags WHERE job_id IN (#job_ids)")
                .cache("dags")

        //generate DAGs
        dh.openCache()
            .get("SELECT A.job_id, A.task_id, B.command_id, B.command_text, B.upstream_ids FROM tasks A INNER JOIN dags B ON A.job_id=B.job_id")
                .put("INSERT INTO qross_tasks_dags (job_id, task_id, command_id, command_text, upstream_ids) VALUES (?, ?, ?, ?, ?)")
        //update tasks status
            .get(s"SELECT (CASE WHEN dependencies>0 THEN '${TaskStatus.INITIALIZED}' ELSE '${TaskStatus.READY}' END) AS status, task_id FROM tasks")
                .put("UPDATE qross_tasks SET status=? WHERE id=?")

        //Master will can't turn on job if no commands to execute - 2018.9.8
        dh.openCache()
            .get("SELECT A.task_id FROM tasks A LEFT JOIN dags B ON A.job_id=B.job_id WHERE B.job_id IS NULL")
                .put(s"UPDATE qross_tasks SET status='${TaskStatus.NO_COMMANDS}' WHERE id=?")

        // ---------- finishing ----------

        //send initialized tasks to checker, and send ready tasks to starter
        val prepared = dh.openDefault().executeDataTable(s"SELECT id AS task_id, job_id, status FROM qross_tasks WHERE status='${TaskStatus.INITIALIZED}' or status='${TaskStatus.READY}'")

        //beat
        dh.executeNonQuery("UPDATE qross_keeper_beats SET last_beat_time=NOW() WHERE actor_name='TaskProducer'")

        writeMessage("TaskProducer beat!")

        dh.close()

        prepared
    }

    def createInstantTask(queryId: String, message: String): Task = {

        /*
            {
                jobId: 123,
                dag: "1,2,3",
                params: "name1:value1,name2:value2",
                commands: "commandId:commandText##$##commandId:commandText"
            }
         */

        val info = Json(message).findDataRow("/")

        var taskId = 0L
        var status = TaskStatus.EMPTY

        val jobId = info.getInt("jobId")
        val dag = info.getString("dag")
        val params = Common.parseMapString(info.getString("params"), ",", ":")
        val commands = Common.parseMapString(info.getString("commands"), "##\\$##", ":")

        //maybe convert failure
        if (jobId > 0) {

            val taskTime = DateTime.now.getString("yyyyMMddHHmmss")

            val dh = new DataHub()

            //create task
            dh.set(s"INSERT INTO qross_tasks (job_id, task_time, status) VALUES ($jobId, '$taskTime', '${TaskStatus.INSTANT}')")
            //get task id
            taskId = dh.executeSingleValue(s"SELECT id FROM qross_tasks WHERE job_id=$jobId AND task_time='$taskTime' AND status='${TaskStatus.INSTANT}'").getOrElse("0").toLong

            if (taskId > 0) {
                //dependencies
                dh.get(s"SELECT job_id, $taskId AS task_id, '$taskTime' AS task_time, id AS dependency_id, dependency_moment, dependency_type, dependency_value  FROM qross_jobs_dependencies WHERE job_id=$jobId")
                    .generateDependencies()

                //DAG
                dh.get(s"SELECT job_id, $taskId AS task_id, id AS command_id, command_text, upstream_ids FROM qross_jobs_dags WHERE job_id=$jobId" + (if (dag != "") s" AND id IN ($dag)" else ""))
                //replace params and commands
                if (params.nonEmpty || commands.nonEmpty) {
                    dh.foreach(row => {
                        if (commands.nonEmpty && commands.contains(row.getString("command_id"))) {
                            row.set("command_text", commands(row.getString("command_id")))
                        }
                        for ((name, value) <- params) {
                            row.set("command_text", row.getString("command_text").replace("${" + name + "}", value))
                        }
                    })
                }
                dh.put("INSERT INTO qross_tasks_dags (job_id, task_id, command_id, command_text, upstream_ids) VALUES (?, ?, ?, ?, ?)")

                //upstream_ids
                if (dag != "") {
                    dh.get(s"SELECT id FROM qross_jobs_dags WHERE job_id=$jobId AND id NOT IN ($dag)")
                        .put(s"UPDATE qross_tasks_dags SET upstream_ids=REPLACE(upstream_ids, '(#id)', '') WHERE task_id=$taskId")
                }

                //task status
                dh.get(s"SELECT id FROM qross_tasks_dependencies WHERE task_id=$taskId AND dependency_moment='before' LIMIT 1")
                status = if (dh.isEmpty) TaskStatus.READY else TaskStatus.INITIALIZED
                dh.set(s"UPDATE qross_tasks SET status='$status' WHERE id=$taskId")
            }

            dh.set(s"INSERT INTO qross_query_result (query_id, result) VALUES ('$queryId', '$taskId')")

            dh.close()

            TaskRecord.of(jobId, taskId).debug(s"Instant Task $taskId of job $jobId has been created.")
        }

        Task(taskId, status)
    }

    def restartTask(taskId: Long, option: String): Task = {

        //Reset task status to RESTARTING in master
        //Reset action status to WAITING
        //Return status: initialized or ready

        //UPDATE qross_tasks SET status=''restarting'',start_time=NULL,finish_time=NULL,spent=NULL WHERE id=#{taskId}

        //INSERT INTO qross_message_box (message_type, message_key, message_text) VALUES ('TASK', 'RESTART', 'WHOLE@69')
        //INSERT INTO qross_message_box (message_type, message_key, message_text) VALUES ('TASK', 'RESTART', '3,4,5@69')
        //INSERT INTO qross_message_box (message_type, message_key, message_text) VALUES ('TASK', 'RESTART', '^EXCEPTIONAL@595052')

        //A. WHOLE: Restart whole task on FINISHED or INCORRECT or FAILED -  reset dependencies，reset dags
        //    option = WHOLE
        //B. ANY: Restart from one or more DONE action on FINISHED/FAILED/INCORRECT - keep dependencies, renew dags
        //        update all dependencies of the action to empty
        //   option =  ^CommandIDs - ^1,2,3,4,5
        //C. EXCEPTIONAL: Restart from all EXCEPTIONAL action on FAILED - keep dependencies, keep dags
        //   option = ^EXCEPTIONAL
        //D. PARTIAL: Restart one or more DONE action only on FINISHED/FAILED/INCORRECT -  keep dependencies, keep dags
        //   option = Commands - 1,2,3,4,5

        //restartMode
        val WHOLE = 1
        val PARTIAL = 2
        val EXCEPTIONAL = 3
        val ANY = 4

        val restartMode = if (option.toUpperCase() == "WHOLE") {
            WHOLE
        }
        else if (option.startsWith("^")) {
            if (option.toUpperCase() == "^EXCEPTIONAL") {
                EXCEPTIONAL
            }
            else {
                ANY
            }
        }
        else {
            PARTIAL
        }

        val dh = new DataHub()

        val row = dh.executeDataRow(s"SELECT job_id, status FROM qross_tasks WHERE id=$taskId")
        var status = row.getString("status")
        val jobId = row.getInt("job_id")

        if (status == TaskStatus.RESTARTING) {
            restartMode match {
                case WHOLE =>
                    //generate dependencies
                    dh.get(s"""SELECT $jobId AS job_id, $taskId AS task_id, A.task_time, B.id AS dependency_id, B.dependency_moment, B.dependency_type, B.dependency_value
                    FROM (SELECT id, job_id, task_time FROM qross_tasks WHERE id=$taskId) A
                     INNER JOIN qross_jobs_dependencies B ON B.job_id=$jobId AND A.job_id=B.job_id""")
                        .fit(s"DELETE FROM qross_tasks_dependencies WHERE task_id=$taskId")
                        .generateDependencies()

                    //generate dags
                    dh.get(s"SELECT A.job_id, A.id AS task_id, B.id AS command_id, B.command_text, B.upstream_ids FROM qross_tasks A INNER JOIN qross_jobs_dags B ON B.job_id=$jobId AND A.id=$taskId AND A.job_id=B.job_id")
                        .fit(s"DELETE FROM qross_tasks_dags WHERE task_id=$taskId")
                        .put("INSERT INTO qross_tasks_dags (job_id, task_id, command_id, command_text, upstream_ids) VALUES (?, ?, ?, ?, ?)")
                case PARTIAL => dh.set(s"UPDATE qross_tasks_dags SET status='waiting' WHERE task_id=$taskId AND command_id IN ($option)")
                case EXCEPTIONAL => dh.set(s"UPDATE qross_tasks_dags SET status='waiting' WHERE task_id=$taskId AND status IN ('exceptional', 'overtime')")
                case ANY =>
                    //refresh upstream_ids
                    dh.get(s"SELECT id, upstream_ids FROM qross_jobs_dags WHERE job_id=$jobId")
                        .put(s"UPDATE qross_tasks_dags SET upstream_ids='#upstream_ids',status='waiting' WHERE task_id=$taskId AND command_id=#id")
                    val commandIds = option.drop(1)
                    dh.get(s"SELECT id FROM qross_jobs_dags WHERE job_id=$jobId WHERE id NOT IN ($commandIds)")
                        .put(s"UPDATE qross_tasks_dags SET upstream_ids=REPLACE(upstream_ids, '(#id)', '') WHERE task_id=$taskId")
                    /* 2018/09/11 so tedious
                    while (dh.get(s"SELECT id, command_id FROM qross_tasks_dags WHERE task_id=$taskId AND upstream_ids='' AND status='waiting' AND command_id NOT IN ($commandIds)").nonEmpty) {
                        dh.put(s"UPDATE qross_tasks_dags SET upstream_ids=REPLACE(upstream_ids, '(#command_id)', '') WHERE task_id=$taskId")
                            .put("UPDATE qross_tasks_dags SET status='done' WHERE id=#id")
                    }*/
            }

            //final status
            status = if (restartMode == WHOLE && dh.executeExists(s"SELECT id FROM qross_jobs_dependencies WHERE job_id=$jobId AND dependency_moment='before' LIMIT 1")) {
                        TaskStatus.INITIALIZED
                    }
                    else {
                        TaskStatus.READY
                    }
            dh.set(s"UPDATE qross_tasks SET status='$status' WHERE id=$taskId")
        }

        dh.close()

        TaskRecord.of(jobId, taskId).debug(s"Task $taskId of job $jobId restart with option $option.")

        Task(taskId, status)
    }

    //TaskChecker
    def checkTaskDependencies(taskId: Long): Boolean = {
        val dh = new DataHub()

        //update task status to ready if all dependencies are ready
        dh.set(s"UPDATE qross_tasks SET status='${TaskStatus.READY}' WHERE id=$taskId AND NOT EXISTS (SELECT task_id FROM qross_tasks_dependencies WHERE task_id=$taskId AND dependency_moment='before' AND ready='no')")

        var jobId: Int = 0
        var status: String = TaskStatus.INITIALIZED

        //check dependencies
        dh.openDefault()
            .get(
                s"""SELECT A.id, A.task_id, A.dependency_type, A.dependency_value, A.ready, B.task_time, A.job_id
                    FROM (SELECT id, job_id, task_id, dependency_type, dependency_value, ready FROM qross_tasks_dependencies WHERE task_id=$taskId AND dependency_moment='before' AND ready='no') A
                    INNER JOIN (SELECT id, task_time FROM qross_tasks where id=$taskId) B ON A.task_id=B.id""")
            .foreach(row => {
                val result = TaskDependency.check(row.getString("dependency_type"), row.getString("dependency_value"), taskId)
                row.set("ready", result._1)
                row.set("dependency_value", result._2)
                jobId = row.getInt("job_id")
                TaskRecord.of(jobId, taskId).log(s"Task $taskId dependency ${row.getLong("id")} of job ${row.getInt("job_id")} is ${if (result._1 == "no") "not " else ""}ready.")
            }).cache("dependencies")

        //update status and others after checking
        dh.openCache()
            .get("SELECT id FROM dependencies WHERE ready='yes'")
                .put("UPDATE qross_tasks_dependencies SET ready='yes' WHERE id=#id")
            .get("SELECT dependency_value, id FROM dependencies WHERE ready='no'")
                .put("UPDATE qross_tasks_dependencies SET dependency_value=?, retry_times=retry_times+1 WHERE id=?")
            //.get("SELECT DISTINCT task_id FROM dependencies WHERE NOT EXISTS (SELECT id FROM dependencies WHERE ready='no')")
            .get("SELECT task_id FROM dependencies WHERE ready='no' GROUP BY task_id HAVING COUNT(0)=0")
                .put(s"UPDATE qross_tasks SET status='${TaskStatus.READY}' WHERE id=#task_id")

        if (dh.nonEmpty) {
            status = TaskStatus.READY
        }
        else {
            //check for checking limit
            dh.openDefault()
                    .get(s"""SELECT A.task_id, A.retry_times, B.retry_limit, A.job_id
                        FROM (SELECT id AS task_id, job_id, dependency_id, retry_times FROM qross_tasks_dependencies WHERE task_id=$taskId AND dependency_moment='before' AND ready='no') A
                        INNER JOIN (SELECT id, retry_limit FROM qross_jobs_dependencies WHERE job_id=$jobId) B ON A.dependency_id=B.id AND B.retry_limit>0 AND A.retry_times>=B.retry_limit""")

            if (dh.nonEmpty) {

                status = TaskStatus.CHECKING_LIMIT
                TaskRecord.of(jobId, taskId).warn(s"Task $taskId of job $jobId reached upper limit of checking limit.")

                //update status
                dh.join(s"""SELECT A.title, A.owner, B.job_id, B.task_time
                        FROM (SELECT id, title, owner FROM qross_jobs WHERE id=$jobId) A
                        INNER JOIN (SELECT job_id, task_time FROM qross_tasks WHERE id=$taskId) B ON A.id=B.job_id""", "job_id" -> "job_id")
                        .cache("task")
                        .put(s"UPDATE qross_tasks SET status='${TaskStatus.CHECKING_LIMIT}', checked='no' WHERE id=$taskId")

                //execute event
                dh.get(s"SELECT job_id, event_function, event_value FROM qross_jobs_events WHERE job_id=$jobId AND enabled='yes' AND event_name='onTaskCheckingLimit'")
                if (dh.nonEmpty) {
                    dh.cache("events")

                    dh.openCache()
                        .get("SELECT A.*, B.event_value AS receivers FROM task A INNER JOIN events B ON A.job_id=B.job_id WHERE event_function='SEND_MAIL_TO'")
                            .sendEmail(TaskStatus.CHECKING_LIMIT)
                        .get("SELECT * FROM task WHERE EXISTS (SELECT job_id FROM events WHERE event_function='REQUEST_API')")
                            .requestApi(TaskStatus.CHECKING_LIMIT)
                        .get("SELECT event_value, '' AS restart_time FROM events WHERE event_function='RESTART_CHECKING_AFTER'")
                            .foreach(row => {
                                row.set("restart_time", DateTime.now.plusMinutes(row.getInt("event_value", 30)).getString("yyyyMMddHHmm00"))
                            }).put(s"UPDATE qross_tasks SET restart_time='#restart_time' WHERE id=$taskId")
                }
            }
        }

        dh.close()

        TaskRecord.of(jobId, taskId).log(s"Task $taskId of job $jobId status is ${if (status == TaskStatus.INITIALIZED) "not ready" else status} after pre-dependencies checking.")

        status == TaskStatus.READY
    }

    def checkTasksStatus(tick: String): Unit = {
        val dh = new DataHub()

        //tasks to be restart
        dh.get(s"SELECT id AS task_id, status FROM qross_tasks WHERE restart_time='$tick'")
        if (dh.nonEmpty) {
            dh.cache("tasks")
            dh.openCache()
                .get(s"SELECT task_id FROM tasks WHERE status='${TaskStatus.CHECKING_LIMIT}'")
                    .put("UPDATE qross_tasks SET retry_times=0 WHERE id=#task_id")
                .get(s"SELECT task_id FROM tasks WHERE status='${TaskStatus.INCORRECT}'")
                    .put(s"UPDATE qross_tasks SET status='${TaskStatus.RESTARTING}', start_time=NULL, finish_time=NULL, spent=NULL, restart_time=NULL WHERE id=#task_id")
                    .put("INSERT INTO qross_message_box (message_type, message_key, message_text) VALUES ('TASK', 'RESTART', 'WHOLE@#task_id')")
                .get(s"SELECT task_id FROM tasks WHERE status='${TaskStatus.FAILED}' OR status='${TaskStatus.TIMEOUT}'")
                    .put(s"UPDATE qross_tasks SET status='${TaskStatus.RESTARTING}', start_time=NULL, finish_time=NULL, spent=NULL, restart_time=NULL WHERE id=#task_id")
                    .put("INSERT INTO qross_message_box (message_type, message_key, message_text) VALUES ('TASK', 'RESTART', '^EXCEPTIONAL@#task_id')")
        }
        dh.clear()

        //recent tasks status
        dh.openDefault()
            .get(s"""SELECT job_id FROM qross_tasks WHERE update_time>='${DateTime(tick).minusMinutes(1).getString("yyyy-MM-dd HH:mm:ss")}'
                    UNION SELECT id AS job_id FROM qross_jobs WHERE recent_tasks_status IS NULL""")
        if (dh.nonEmpty) {
            dh.pass("SELECT job_id, GROUP_CONCAT(CONCAT(id, ':', status, '@', task_time) ORDER BY id DESC SEPARATOR ',') AS status FROM (SELECT job_id, id, status, task_time FROM qross_tasks WHERE job_id=#job_id ORDER BY id DESC LIMIT 3) T GROUP BY job_id")
                    .put("UPDATE qross_jobs SET recent_tasks_status='#status' WHERE id=#job_id")
        }

        writeMessage("TaskStarter beat!")
        dh.set(s"UPDATE qross_keeper_beats SET last_beat_time=NOW() WHERE actor_name='TaskStarter'")

        dh.close()
    }

    //TaskStarter - beat()
    /*
    def getManualCommandsToExecute(tick: String): DataTable = {
        val minute = DateTime(tick)
        val ds = new DataSource()
        //commands
        val executable = ds.executeDataTable(commandsBaseSQL
            + " WHERE B.status IN ('finished', 'failed', 'incorrect') AND A.status IN ('restarting', 'manual')")
    
        writeMessage("TaskStarter beat!")
        ds.executeNonQuery(s"UPDATE qross_keeper_beats SET last_beat_time=NOW() WHERE actor_name='TaskStarter'")
        ds.close()
    
        executable
    }
    def checkOvertimeOfActions(tick: String): Unit = {
        val minute = DateTime(tick)
        val dh = new DataHub()
        
        dh.get("SELECT A.id AS action_id, A.job_id, A.task_id, A.command_id, B.command_text, B.overtime, C.title, C.owner, C.mail_notification, C.mail_master_on_exception, D.task_time FROM qross_tasks_dags A INNER JOIN qross_jobs_dags B ON A.status='running' AND A.job_id=B.job_id AND B.overtime>0 AND TIMESTAMPDIFF(SECOND, A.update_time, NOW())>B.overtime INNER JOIN qross_jobs C ON A.job_id=C.id AND C.enabled='yes' INNER JOIN qross_tasks D ON A.task_id=D.id")
        if (dh.nonEmpty) {
            dh.put("UPDATE qross_tasks_dags SET status='timeout' WHERE id=#action_id")
                .put("UPDATE qross_tasks SET status='timeout', checked='no' WHERE id=#task_id")
    
            if (Global.EMAIL_NOTIFICATION) {
                dh.foreach(row => {
                    if (row.getBoolean("mail_notification") && row.getString("owner", "") != "") {
                        OpenResourceFile("/templates/timeout.html")
                            .replaceWith(row)
                            .writeEmail(s"TIMEOUT: ${row.getString("title")} ${row.getString("task_time")} - TaskID: ${row.getString("task_id")}")
                            .to(row.getString("owner"))
                            .cc(if (row.getBoolean("mail_master_on_exception")) Global.MASTER_USER_GROUP else "")
                            .send()
                    }
                })
            }
        }
    
        writeMessage("TaskStarter beat!")
        dh.set(s"UPDATE qross_keeper_beats SET last_beat_time=NOW() WHERE actor_name='TaskStarter'")
        dh.close()
    }
    */

}