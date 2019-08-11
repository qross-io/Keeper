package io.qross.model

import io.qross.core.{DataHub, DataRow, DataTable, DataType}
import io.qross.ext.Output._
import io.qross.ext.TypeExt._
import io.qross.fs.ResourceFile
import io.qross.jdbc.DataSource
import io.qross.net.Json
import io.qross.setting.Global
import io.qross.sql.PSQL
import io.qross.time.{CronExp, DateTime, Timer}
import io.qross.sql.Solver._

import scala.collection.mutable
import scala.sys.process._
import scala.util.{Success, Try}

object QrossTask {

    val TO_BE_KILLED: mutable.HashSet[Long] = new mutable.HashSet[Long]()

    implicit class DataHubExt(dh: DataHub) {

        def sendEmail(taskStatus: String): DataHub = {
            val upperStatus = taskStatus.toUpperCase()
            dh.foreach(row => {
                if (Global.EMAIL_NOTIFICATION) {
                    val receivers = row.getString("receivers")
                    if (receivers != "") {
                        row.set("event_value",
                            ResourceFile.open(s"/templates/$taskStatus.html")
                                .replace("${status}", upperStatus)
                                .replaceWith(row)
                                .replace("${logs}", if (dh.containsBuffer("logs")) TaskRecorder.toHTML(dh.takeOut("logs")) else "")
                                .writeEmail(s"$upperStatus: ${row.getString("title")} ${row.getString("task_time")} - JobID: ${row.getString("job_id")} - TaskID: ${row.getString("task_id")}")
                                .to(if (receivers.contains("_OWNER")) row.getString("owner") else "")
                                .cc(if (receivers.contains("_MASTER")) Global.MASTER_USER_GROUP else "")
                                .cc(if (receivers.contains("_KEEPER")) Global.KEEPER_USER_GROUP else "")
                                .send())

                        TaskRecorder.of(row.getInt("job_id"), row.getLong("task_id"), row.getString("record_time"))
                                .debug(s"Task ${row.getLong("task_id")} of job ${row.getInt("job_id")} at <${row.getString("record_time")}> sent a mail on task $taskStatus")
                    }
                    else {
                        row.set("event_value", "NO RECEIVERS")
                    }
                }
                else {
                    row.set("event_value", "EMAIL NOTIFICATION has closed.")
                }
            }).put("INSERT INTO qross_tasks_events (job_id, task_id, record_time, event_name, event_function, event_value) VALUES (#job_id, #task_id, '#record_time', '#event_name', '#event_function', '#event_value')")
                .clear()
        }

        def requestApi(taskStatus: String): DataHub = {
            dh.foreach(row => {
                var api = row.getString("api")

                if (api == "") {
                    api = taskStatus match {
                        case TaskStatus.NEW => Global.API_ON_TASK_NEW
                        case TaskStatus.CHECKING_LIMIT => Global.API_ON_TASK_CHECKING_LIMIT
                        case TaskStatus.READY => Global.API_ON_TASK_READY
                        case TaskStatus.FAILED => Global.API_ON_TASK_FAILED
                        case TaskStatus.TIMEOUT => Global.API_ON_TASK_TIMEOUT
                        case TaskStatus.INCORRECT => Global.API_ON_TASK_INCORRECT
                        case TaskStatus.SUCCESS => Global.API_ON_TASK_SUCCESS
                        case _ => ""
                    }
                }

                if (api != "") {
                    var data = ""
                    var path = "/"

                    //api格式 method @ api
                    if (api.contains("@")) {
                        data = api.takeBefore("@").trim.takeAfter("""(?i)^POST\s""".r).toUpperCase()
                        api = api.takeAfter("@").trim
                    }
                    if (api.contains("->")) {
                        path = api.takeAfter("->").trim
                        api = api.takeBefore("->").trim
                    }

                    api = api.replace("${jobId}", row.getString("job_id", "0"))
                            .replace("${taskId}", row.getString("task_id", "0"))
                            .replace("${commandId}", row.getString("command_id", "0"))
                            .replace("${actionId}", row.getString("action_id", "0"))
                            .replace("${status}", taskStatus)
                            .replace("${title}", row.getString("title"))
                            .replace("${retryTimes}", row.getString("retry_times", "0"))
                            .replace("${retryLimit}", row.getString("retry_limit", "0"))
                            .replace("${owner}", row.getString("owner"))
                            .replace("${taskTime}", row.getString("task_time"))

                    val result = try {
                            Json.fromURL(api, data).parseValue(path)
                        }
                        catch {
                            case e: Exception => e.getMessage
                            case _: Throwable => ""
                        }

                    TaskRecorder.of(row.getInt("job_id"), row.getLong("task_id"), row.getString("record_time"))
                            .debug(s"Task ${row.getLong("task_id")} of job ${row.getInt("job_id")} at <${row.getString("record_time")}> requested api on task $taskStatus, result is { $result }")
                }

                row.set("event_value", if (api == "") "EMPTY" else api)
            }).put("INSERT INTO qross_tasks_events (job_id, task_id, record_time, event_name, event_function, event_value) VALUES (#job_id, #task_id, '#record_time', '#event_name', '#event_function', '#event_value')")
                .clear()
        }

        def runPSQL(taskStatus: String): DataHub = {

            dh.foreach(row => {
                val SQL = row.getString("event_value").replaceArguments(Map[String, String](
                    "jobId" -> row.getString("job_id", "0"),
                    "taskId" -> row.getString("task_id", "0"),
                    "commandId" -> row.getString("command_id", "0"),
                    "actionId" -> row.getString("action_id", "0"),
                    "status" -> taskStatus,
                    "title" -> row.getString("title"),
                    "retryTimes" -> row.getString("retry_times", "0"),
                    "retryLimit" -> row.getString("retry_limit", "0"),
                    "owner" -> row.getString("owner"),
                    "taskTime" -> row.getString("task_time")
                ))

                //execute
                new PSQL(SQL, dh).$run()

                TaskRecorder.of(row.getInt("job_id"), row.getLong("task_id"), row.getString("record_time"))
                        .debug(s"Task ${row.getLong("task_id")} of job ${row.getInt("job_id")} at <${row.getString("record_time")}> execute PSQL on task $taskStatus.")

            }).put("INSERT INTO qross_tasks_events (job_id, task_id, record_time, event_name, event_function, event_value) VALUES (#job_id, #task_id, '#record_time', '#event_name', '#event_function', '#event_value')")
                .clear()
        }

        def generateDependencies(): DataHub = {
            if (dh.nonEmpty) {
                val table = DataTable.withFields("job_id" -> DataType.INTEGER,
                                                        "task_id" -> DataType.INTEGER,
                                                        "record_time" -> DataType.TEXT,
                                                        "dependency_id" -> DataType.INTEGER,
                                                        "dependency_moment" -> DataType.TEXT,
                                                        "dependency_type" -> DataType.TEXT,
                                                        "dependency_value" -> DataType.TEXT)

                dh.foreach(row => {
                    TaskDependency.parseDependencyValue(
                        row.getString("job_id"),
                        row.getString("task_id"),
                        row.getString("dependency_value"),
                        row.getString("task_time"))
                            .foreach(value =>
                                table.insertRow("job_id" -> row.getInt("job_id"),
                                    "task_id" -> row.getLong("task_id"),
                                    "record_time" -> row.getString("record_time"),
                                    "dependency_id" -> row.getInt("dependency_id"),
                                    "dependency_moment" -> row.getString("dependency_moment"),
                                    "dependency_type" -> row.getString("dependency_type"),
                                    "dependency_value" -> value)
                            )
                })

                dh.put("INSERT INTO qross_tasks_dependencies (job_id, task_id, record_time, dependency_id, dependency_moment, dependency_type, dependency_value) VALUES (?, ?, ?, ?, ?, ?, ?)", table)
            }

            dh.clear()
        }

        def restartTask(taskStatus: String): DataHub = {

            if (dh.nonEmpty) {
                dh.put("INSERT INTO qross_tasks_events (job_id, task_id, record_time, event_name, event_function, event_value) VALUES (#job_id, #task_id, '#record_time', '#event_name', '#event_function', '')")
                dh.foreach(row => {
                    val delay = row.getInt("delay", 30)
                    row.set("to_be_start_time", DateTime.now.plusMinutes(delay).getString("yyyyMMddHHmm00"))

                    TaskRecorder.of(row.getInt("job_id"), row.getLong("task_id"), row.getString("record_time")).debug(s"Task ${row.getLong("task_id")} of job ${row.getInt("job_id")} at <${row.getString("record_time")}> will restart after $delay minutes by $taskStatus.").dispose()
                })
                .put("UPDATE qross_tasks SET to_be_start_time='#to_be_start_time' WHERE id=#task_id")
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

        val dh = DataHub.Qross

        //get last tick of producer
        val lastBeat = (Try(dh.executeSingleValue("SELECT UNIX_TIMESTAMP(last_beat_time) FROM qross_keeper_beats WHERE actor_name='TaskProducer'").asInteger) match {
                case Success(tick) => DateTime.of(tick)
                case _ => DateTime.now
            }).setSecond(0).setNano(0).plusMinutes(1)
        val nextBeat = DateTime.now.setSecond(0).setNano(0).plusMinutes(1)

        //get all jobs
        dh.openQross()
            //next_tick != '' means this is not a new job
            .get(s"SELECT id AS job_id, cron_exp, next_tick, complement_missed_tasks FROM qross_jobs WHERE enabled='yes' AND cron_exp<>'' AND next_tick<>'' AND next_tick<>'NONE' AND next_tick<${nextBeat.getTickValue}")
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
                            writeException(e.getMessage)
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
        dh.openQross()
            .get(s"SELECT job_id, task_time FROM qross_tasks WHERE job_id IN (SELECT id FROM qross_jobs WHERE enabled='yes' AND cron_exp<>'' AND next_tick<>'' AND next_tick<>'NONE' AND complement_missed_tasks='yes') AND task_time>${lastBeat.getTickValue} AND task_time<${nextBeat.getTickValue}")
                .cache("exists_tasks")

        //complement all jobs
        dh.openCache()
            .get(s"SELECT A.job_id, A.next_tick FROM missed_tasks A LEFT JOIN exists_tasks B ON A.job_id=B.job_id AND A.next_tick=B.task_time WHERE B.job_id IS NULL")
                .put(s"INSERT INTO qross_tasks (job_id, task_time, record_time, creator, create_mode, start_mode) VALUES (?, ?, '${DateTime.now}', '#keeper', 'complement', 'auto_start')")

        //get next tick for all jobs
        dh.openCache()
            .get("SELECT job_id, cron_exp, next_tick FROM jobs")
                .foreach(row => {
                    try {
                        row.set("next_tick", CronExp(row.getString("cron_exp")).getNextTickOrNone(nextBeat))
                    }
                    catch {
                        case e: Exception => writeException(e.getMessage)
                    }
                }).put("UPDATE qross_jobs SET next_tick='#next_tick' WHERE id=#job_id")

        //restart executing tasks when Keeper exit exceptionally.
        dh.openQross()
             .get(s"SELECT A.task_id, A.record_time FROM (SELECT id AS task_id, job_id, record_time FROM qross_tasks WHERE status='${TaskStatus.EXECUTING}') A INNER JOIN qross_jobs B ON A.job_id=B.id")
                .put(s"UPDATE qross_tasks_dags SET status='${ActionStatus.EXCEPTIONAL}' WHERE task_id=#task_id AND record_time='#record_time' AND status IN ('${ActionStatus.QUEUEING}', '${ActionStatus.RUNNING}')")
                .put("INSERT INTO qross_message_box (message_type, message_key, message_text) VALUES ('TASK', 'RESTART', '^EXCEPTIONAL@#task_id')")

        dh.close()
    }

    //TaskProducer
    //create and initialize tasks then return initialized and ready tasks
    def createAndInitializeTasks(tick: String): DataTable = {
        val minute = DateTime(tick)

        val dh = DataHub.Qross

        //update empty next_tick - it will be empty when create a new job
        //update outdated jobs - it will occur when you enable a job from disabled
        dh
            //next_tick will be NONE if cron exp is expired.
            .get(s"SELECT id AS job_id, cron_exp, '' AS next_tick FROM qross_jobs WHERE cron_exp<>'' AND (next_tick='' OR (next_tick<>'NONE' AND next_tick<$tick))")
                .foreach(row =>
                    try {
                        row.set("next_tick", CronExp(row.getString("cron_exp")).getNextTickOrNone(minute))
                    }
                    catch {
                        case e: Exception => writeException(e.getMessage)
                    }
                ).put("UPDATE qross_jobs SET next_tick='#next_tick' WHERE id=#job_id")

        //create tasks without cron_exp
        //excluding jobs with executing tasks
        dh.set(
                s"""INSERT INTO qross_tasks (job_id, task_time, record_time, creator, create_mode, start_mode) SELECT id, '', '${DateTime.now}', '#keeper', 'schedule', 'auto_start' FROM qross_jobs
                     WHERE cron_exp='' AND enabled='yes' AND id NOT IN (SELECT DISTINCT job_id FROM qross_tasks WHERE task_time=''
                      AND status NOT IN ('${TaskStatus.FINISHED}', '${TaskStatus.INCORRECT}', '${TaskStatus.FAILED}', '${TaskStatus.TIMEOUT}', '${TaskStatus.SUCCESS}'))""")

        //get next minute to match next tick
        minute.plusMinutes(1)
        //jobs with cron_exp
        dh.get(s"SELECT id AS job_id, cron_exp, next_tick FROM qross_jobs WHERE next_tick='$tick' AND enabled='yes' AND id NOT IN (SELECT job_id FROM qross_tasks WHERE task_time='$tick')")
            //create schedule tasks
                .put(s"INSERT INTO qross_tasks (job_id, task_time, record_time, creator, create_mode, start_mode) VALUES (#job_id, '#next_tick', '${DateTime.now}', '#keeper', 'schedule', 'auto_start')")
            //get next tick and update
            .foreach(row => {
                try {
                    row.set("next_tick", CronExp(row.getString("cron_exp")).getNextTickOrNone(minute))
                }
                catch {
                    case e: Exception => writeException(e.getMessage)
                }
            }).put("UPDATE qross_jobs SET next_tick='#next_tick' WHERE id=#job_id")

        //get all new tasks
        dh.get(
                s"""SELECT A.task_id, A.job_id, C.title, C.owner, A.task_time, A.record_time, IFNULL(B.dependencies, 0) AS dependencies
                   FROM (SELECT id AS task_id, job_id, task_time, record_time FROM qross_tasks WHERE status='${TaskStatus.NEW}') A
                   INNER JOIN qross_jobs C ON A.job_id=C.id
                   LEFT JOIN (SELECT job_id, COUNT(0) AS dependencies FROM qross_jobs_dependencies WHERE dependency_moment='before' GROUP BY job_id) B ON A.job_id=B.job_id""")
                .cache("tasks")

        if (dh.nonEmpty) {
            //onTaskNew events
            dh.openCache()
                //update status
                .get("SELECT GROUP_CONCAT(job_id) AS job_ids FROM tasks")
            .openQross()
                .pass("SELECT job_id, event_name, event_function, event_value FROM qross_jobs_events WHERE job_id IN (#job_ids) AND enabled='yes' AND event_name='onTaskNew'")
                    .cache("events")

            dh.openCache()
                .get("SELECT A.task_id, A.job_id, A.title, A.owner, A.task_time, A.record_time, B.event_value AS receivers, B.event_name, B.event_function, '' AS event_value FROM tasks A INNER JOIN events B ON A.job_id=B.job_id AND B.event_function='SEND_MAIL_TO'")
                    .sendEmail(TaskStatus.NEW)
                .get("SELECT A.task_id, A.job_id, A.title, A.owner, A.task_time, A.record_time, B.event_value AS api, B.event_name, B.event_function, '' AS event_value FROM tasks A INNER JOIN events B ON A.job_id=B.job_id AND event_function='REQUEST_API'")
                    .requestApi(TaskStatus.NEW)

            // ----- dependencies -----

            //get all dependencies
            dh.openCache()
                .get("SELECT IFNULL(GROUP_CONCAT(DISTINCT job_id), 0) AS job_ids FROM tasks WHERE dependencies>0")
            dh.openQross()
                .pass("SELECT id AS dependency_id, job_id, dependency_moment, dependency_type, dependency_value FROM qross_jobs_dependencies WHERE job_id IN (#job_ids)")
                    .cache("dependencies")

            //generate dependencies
            dh.openCache()
                    .get("SELECT A.job_id, A.task_id, A.task_time, A.record_time, B.dependency_id, B.dependency_moment, B.dependency_type, B.dependency_value FROM tasks A INNER JOIN dependencies B ON A.job_id=B.job_id")
                    .generateDependencies()

            // ---------- DAGs ----------

            //get all DAGs
            dh.openCache()
                .get("SELECT IFNULL(GROUP_CONCAT(DISTINCT job_id), 0) AS job_ids FROM tasks")
            dh.openQross()
                .pass("SELECT id AS command_id, command_text, job_id, upstream_ids FROM qross_jobs_dags WHERE job_id IN (#job_ids)")
                    .cache("dags")

            //generate DAGs
            dh.openCache()
                    .get("SELECT A.job_id, A.task_id, A.record_time, B.command_id, B.command_text, B.upstream_ids FROM tasks A INNER JOIN dags B ON A.job_id=B.job_id")
                    .put("INSERT INTO qross_tasks_dags (job_id, task_id, record_time, command_id, command_text, upstream_ids) VALUES (?, ?, ?, ?, ?, ?)")

            //update tasks status
            dh.get(s"SELECT task_id, (CASE WHEN dependencies>0 THEN '${TaskStatus.INITIALIZED}' ELSE '${TaskStatus.READY}' END) AS status FROM tasks")
                    .put("UPDATE qross_tasks SET status='#status' WHERE id=#task_id")

            //Master will can't turn on job if no commands to execute - 2018.9.8
            dh.get("SELECT A.task_id FROM tasks A LEFT JOIN dags B ON A.job_id=B.job_id WHERE B.job_id IS NULL")
                    .put(s"UPDATE qross_tasks SET status='${TaskStatus.NO_COMMANDS}' WHERE id=#task_id")

        }
        // ---------- finishing ----------

        //send initialized tasks to checker, and send ready tasks to starter
        val prepared = dh.openQross()
                .executeDataTable(s"SELECT id As task_id, job_id, task_time, record_time, status FROM qross_tasks WHERE status='${TaskStatus.INITIALIZED}' OR status='${TaskStatus.READY}'")

        //beat
        dh.set("UPDATE qross_keeper_beats SET last_beat_time=NOW() WHERE actor_name='TaskProducer'")
        writeMessage("TaskProducer beat!")

        dh.close()

        prepared
    }

    def createInstantTask(queryId: String, message: String): Option[Task] = {

        /*
            {
                jobId: 123,
                dag: "1,2,3",
                params: "name1:value1,name2:value2",
                commands: "commandId:commandText##$##commandId:commandText",
                delay: 5,  //s
                start_time: 'yyyyMMddHHmm00',
                creator: @creator
            }
         */

        val info = Json(message).parseRow("/")

        var status = TaskStatus.EMPTY

        val jobId = info.getInt("jobId")
        var taskId = 0L
        val dag = info.getString("dag")
        val params = info.getString("params").toHashMap(",", ":")
        val commands = info.getString("commands").toHashMap("##\\$##", ":")
        val delay = info.getInt("delay")
        val taskTime = DateTime.now.getString("yyyyMMddHHmmss")
        val recordTime = DateTime.now.toString()
        val startTime = info.getString("start_time")
        val creator = info.getString("creator")

        //maybe convert failure
        if (jobId > 0) {

            val dh = DataHub.Qross

            //create task
            dh.set(s"INSERT INTO qross_tasks (job_id, task_time, record_time, status, creator, create_mode, start_mode, to_be_start_time) VALUES ($jobId, '$taskTime', '$recordTime', '${TaskStatus.INSTANT}', '$creator', 'instant', 'manual_start', '$startTime')")
            //get task id
            dh.get(s"""SELECT A.task_id, A.job_id, A.task_time, A.record_time, B.title, B.owner, IFNULL(C.dependencies, 0) AS dependencies
                        FROM (SELECT id AS task_id, job_id, task_time, record_time FROM qross_tasks WHERE job_id=$jobId AND task_time='$taskTime' AND status='${TaskStatus.INSTANT}') A
                        INNER JOIN qross_jobs B ON A.job_id=B.id
                        LEFT JOIN (SELECT job_id, COUNT(0) AS dependencies FROM qross_jobs_dependencies WHERE job_id=$jobId AND dependency_moment='before' GROUP BY job_id) C ON A.job_id=C.job_id""")
                .cache("task_info")

            taskId = dh.firstRow.getLong("task_id")

            if (taskId > 0) {

                dh.get(s"SELECT job_id, event_name, event_function, event_value FROM qross_jobs_events WHERE job_id=$jobId AND enabled='yes' AND event_name='onTaskNew'")
                    .cache("events")

                //onTaskNew event
                dh.openCache()
                    .get("SELECT A.task_id, A.job_id, A.title, A.owner, A.task_time, A.record_time, B.event_value AS receivers, '' AS event_value, B.event_name, B.event_function FROM task_info A INNER JOIN events B ON A.job_id=B.job_id AND B.event_name='onTaskNew' AND B.event_function='SEND_MAIL_TO'")
                        .sendEmail(TaskStatus.NEW)
                    .get("SELECT A.task_id, A.job_id, A.title, A.owner, A.task_time, A.record_time, B.event_value AS api, '' AS event_value, B.event_name, B.event_function FROM task_info A INNER JOIN events B ON A.job_id=B.job_id AND B.event_name='onTaskNew' AND B.event_function='REQUEST_API'")
                        .requestApi(TaskStatus.NEW)

                //dependencies
                dh.openQross()
                    .get(s"SELECT job_id, $taskId AS task_id, '$taskTime' AS task_time, '$recordTime' AS record_time, id AS dependency_id, dependency_moment, dependency_type, dependency_value FROM qross_jobs_dependencies WHERE job_id=$jobId")
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
                dh.put(s"INSERT INTO qross_tasks_dags (job_id, task_id, record_time, command_id, command_text, upstream_ids) VALUES (?, ?, '$recordTime', ?, ?, ?)")

                //upstream_ids
                if (dag != "") {
                    dh.get(s"SELECT id FROM qross_jobs_dags WHERE job_id=$jobId AND id NOT IN ($dag)")
                        .put(s"UPDATE qross_tasks_dags SET upstream_ids=REPLACE(upstream_ids, '(#id)', '') WHERE task_id=$taskId")
                }

                //task status
                dh.openCache()
                    .get(s"SELECT (CASE WHEN dependencies>0 THEN '${TaskStatus.INITIALIZED}' ELSE '${TaskStatus.READY}' END) AS status FROM task_info")
                        .put(s"UPDATE qross_tasks SET status='#status' WHERE id=$taskId")

                status = dh.firstRow.getString("status")
            }

            dh.openQross().set(s"INSERT INTO qross_query_result (query_id, result) VALUES ('$queryId', '$taskId')")

            dh.close()

            TaskRecorder.of(jobId, taskId, recordTime).debug(s"Instant Task $taskId of job $jobId at <$recordTime> has been created.")
        }

        if (delay > 0 && startTime == "" && jobId  > 0 && taskId > 0) {
            TaskRecorder.of(jobId, taskId, recordTime).debug(s"Instant Task $taskId of job $jobId at <$recordTime> will start after $delay seconds.")
            Timer.sleep(if (delay > 60) 60 else delay)
        }

        if (startTime == "") {
            Some(Task(taskId, status).of(jobId).at(taskTime, recordTime))
        }
        else {
            None
        }
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
        //[REMOVED] B. ANY: Restart from one or more DONE action on FINISHED/FAILED/INCORRECT - keep dependencies, renew dags
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

        val restartMode = if (option.startsWith("^")) "auto_restart" else "manual_restart"
        val restartMethod = if (option.toUpperCase().endsWith("WHOLE")) {
            WHOLE
        }
        else if (option.toUpperCase().endsWith("EXCEPTIONAL")) {
            EXCEPTIONAL
        }
        else {
            PARTIAL
        }

        val dh = DataHub.Qross

        //backup records
        dh.get(s"SELECT job_id, task_time, record_time, start_mode, status, start_time, finish_time, IFNULL(spent, -1) AS spent FROM qross_tasks WHERE id=$taskId")
            .put(s"INSERT INTO qross_tasks_records (job_id, task_id, record_time, start_mode, status, start_time, finish_time, spent) VALUES (#job_id, $taskId, '#record_time', '#start_mode', '#status', '#start_time', '#finish_time', #spent)")

        val row = dh.firstRow

        val jobId = row.getInt("job_id")
        val taskTime = row.getString("task_time")
        val prevRecordTime = row.getString("record_time")
        val recordTime = DateTime.now.toString

        var status = row.getString("status", "EMPTY")

        if (dh.nonEmpty) {

            status = TaskStatus.READY

            restartMethod match {
                case WHOLE =>
                    //generate before and after dependencies
                    dh.get(s"""SELECT $jobId AS job_id, $taskId AS task_id, '$taskTime' AS task_time, '$recordTime' AS record_time, id AS dependency_id, dependency_moment, dependency_type, dependency_value FROM qross_jobs_dependencies WHERE job_id=$jobId""")
                        .cache("dependencies")
                        .generateDependencies()

                    //generate dags
                    dh.get(s"SELECT job_id, $taskId AS task_id, id AS command_id, command_text, upstream_ids FROM qross_jobs_dags WHERE job_id=$jobId")
                        .put(s"INSERT INTO qross_tasks_dags (job_id, task_id, record_time, command_id, command_text, upstream_ids) VALUES (?, ?, '$recordTime', ?, ?, ?)")

                    dh.openCache().get("SELECT task_id FROM dependencies WHERE dependency_moment='before'")
                    if (dh.nonEmpty) {
                        status = TaskStatus.INITIALIZED
                    }

                case PARTIAL =>
                    dh.get(s"SELECT job_id, $taskId AS task_id, id AS command_id, command_text, upstream_ids FROM qross_jobs_dags WHERE job_id=$jobId AND id IN ($option)")
                        .put(s"INSERT INTO qross_tasks_dags (job_id, task_id, record_time, command_id, command_text, upstream_ids) VALUES (?, ?, '$recordTime', ?, ?, ?)")
                    .get(s"SELECT id FROM qross_jobs_dags WHERE job_id=$jobId AND id NOT IN ($option)")
                        .put(s"UPDATE qross_tasks_dags SET upstream_ids=REPLACE(upstream_ids, '(#id)', '') WHERE task_id=$taskId AND record_time='$recordTime")

                case EXCEPTIONAL =>
                    //generate "after" dependencies
                    dh.get(s"SELECT $jobId AS job_id, $taskId AS task_id, '$taskTime' AS task_time, '$recordTime' AS record_time, id AS dependency_id, dependency_moment, dependency_type, dependency_value FROM qross_jobs_dependencies WHERE job_id=$jobId AND dependency_moment='after'")
                        .generateDependencies()

                    dh.get(s"SELECT IFNULL(GROUP_CONCAT(command_id), 0) AS command_ids FROM qross_tasks_dags WHERE task_id=$taskId AND record_time='$prevRecordTime' AND status IN ('exceptional', 'overtime', 'waiting')")
                        .pass(s"SELECT job_id, $taskId AS task_id, id AS command_id, command_text, upstream_ids FROM qross_jobs_dags WHERE job_id=$jobId AND id IN (#command_ids)")
                            .put(s"INSERT INTO qross_tasks_dags (job_id, task_id, record_time, command_id, command_text, upstream_ids) VALUES (?, ?, '$recordTime', ?, ?, ?)")
                        .get(s"SELECT command_id FROM qross_tasks_dags WHERE task_id=$taskId AND record_time='$prevRecordTime' AND status='done'")
                            .put(s"UPDATE qross_tasks_dags SET upstream_ids=REPLACE(upstream_ids, '(#command_id)', '') WHERE task_id=$taskId AND record_time='$recordTime'")
                    //prev version code
                    //dh.set(s"UPDATE qross_tasks_dags SET status='waiting' WHERE task_id=$taskId AND status IN ('exceptional', 'overtime')")


            }

            //update task
            dh.openQross().set(s"UPDATE qross_tasks SET status='$status', start_mode='$restartMode', record_time='$recordTime', spent=NULL, start_time=NULL, finish_time=NULL WHERE id=$taskId")

            TaskRecorder.of(jobId, taskId, recordTime).debug(s"Task $taskId of job $jobId at <$recordTime> restart with option $option.")
        }

        dh.close()

        Task(taskId, status).of(jobId).at(taskTime, recordTime)
    }

    //TaskChecker
    def checkTaskDependencies(task: Task): Boolean = {

        val taskId = task.id
        val jobId = task.jobId
        val recordTime = task.recordTime

        val dh = DataHub.Qross

        //update task status to ready if all dependencies are ready
        dh.set(s"UPDATE qross_tasks SET status='${TaskStatus.READY}' WHERE id=$taskId AND NOT EXISTS (SELECT task_id FROM qross_tasks_dependencies WHERE task_id=$taskId AND record_time='$recordTime' AND dependency_moment='before' AND ready='no')")
        //check dependencies
        dh.openQross()
            .get(
                s"""SELECT A.id, A.task_id, A.dependency_type, A.dependency_value, A.ready, B.task_time
                    FROM (SELECT id, job_id, task_id, dependency_type, dependency_value, ready FROM qross_tasks_dependencies WHERE task_id=$taskId AND record_time='$recordTime' AND dependency_moment='before' AND ready='no') A
                    INNER JOIN (SELECT id, task_time FROM qross_tasks where id=$taskId) B ON A.task_id=B.id""")
            .foreach(row => {
                val result = TaskDependency.check(row.getString("dependency_type"), row.getString("dependency_value"), taskId, recordTime)
                row.set("ready", result._1)
                row.set("dependency_value", result._2)
                if (result._1 == "yes") {
                    TaskRecorder.of(jobId, taskId, recordTime).log(s"Task $taskId of job $jobId at <$recordTime> dependency ${row.getLong("id")} is ready.")
                }
            }).cache("dependencies")

        //update status and others after checking
        dh.openCache()
            .get("SELECT id FROM dependencies WHERE ready='yes'")
                .put("UPDATE qross_tasks_dependencies SET ready='yes' WHERE id=#id")
            .get("SELECT dependency_value, id FROM dependencies WHERE ready='no'")
                .put("UPDATE qross_tasks_dependencies SET dependency_value=?, retry_times=retry_times+1 WHERE id=?")
            .get("SELECT task_id FROM dependencies WHERE ready='no' GROUP BY task_id HAVING COUNT(0)=0")
                .put(s"UPDATE qross_tasks SET status='${TaskStatus.READY}' WHERE id=#task_id")

        var status: String = dh.openQross().executeSingleValue(s"SELECT status FROM qross_tasks WHERE id=$taskId").asText(TaskStatus.INITIALIZED)

        if (status == TaskStatus.INITIALIZED)  {
            //check for checking limit
            dh.openQross()
                .get(s"""SELECT A.task_id, A.retry_times, B.retry_limit, A.job_id
                        FROM (SELECT task_id, job_id, dependency_id, retry_times FROM qross_tasks_dependencies WHERE task_id=$taskId AND record_time='$recordTime' AND dependency_moment='before' AND ready='no') A
                        INNER JOIN (SELECT id, retry_limit FROM qross_jobs_dependencies WHERE job_id=$jobId) B ON A.dependency_id=B.id AND B.retry_limit>0 AND A.retry_times>=B.retry_limit""")

            if (dh.nonEmpty) {
                status = TaskStatus.CHECKING_LIMIT
                dh.set(s"UPDATE qross_tasks SET status='${TaskStatus.CHECKING_LIMIT}', checked='no' WHERE id=$taskId")

                TaskRecorder.of(jobId, taskId, recordTime).warn(s"Task $taskId of job $jobId at <$recordTime> reached upper limit of checking limit.").dispose()

                //update status if reach upper limit
                dh.join(s"""SELECT B.task_id, A.title, A.owner, B.job_id, B.task_time, B.record_time
                            FROM (SELECT id, title, owner FROM qross_jobs WHERE id=$jobId) A
                            INNER JOIN (SELECT id AS task_id, job_id, task_time, record_time FROM qross_tasks WHERE id=$taskId) B ON A.id=B.job_id""", "job_id" -> "job_id")
                        .cache("task_info")


                //execute event
                dh.get(
                    s"""SELECT A.job_id, A.event_name, A.event_function, A.event_value, A.event_option, IFNULL(B.current_option, 0) AS current_option FROM
                        (SELECT job_id, event_name, event_function, event_value, event_option FROM qross_jobs_events WHERE job_id=$jobId AND event_name='onTaskCheckingLimit' AND enabled='yes') A
                            LEFT JOIN
                        (SELECT job_id, event_function, COUNT(0) AS current_option FROM qross_tasks_events WHERE task_id=$taskId AND event_name='onTaskCheckingLimit' AND archived=0 GROUP BY job_id, event_function) B
                        ON A.job_id=B.job_id AND A.event_function=B.event_function
                     """)

                if (dh.nonEmpty) {
                    dh.cache("events")

                    dh.openCache()
                        .get("SELECT A.*, B.event_value AS receivers, '' AS event_value, B.event_name, B.event_function FROM task_info A INNER JOIN events B ON A.job_id=B.job_id WHERE event_function='SEND_MAIL_TO'")
                            .sendEmail(TaskStatus.CHECKING_LIMIT)
                        .get("SELECT A.*, B.event_value AS api, '' AS event_value, B.event_name, B.event_function FROM task_info A INNER JOIN events B ON A.job_id=B.job_id AND B.event_function='REQUEST_API'")
                            .requestApi(TaskStatus.CHECKING_LIMIT)
                        .get("SELECT A.*, B.event_value AS delay, B.event_name, B.event_function, '' AS to_be_start_time FROM task_info A INNER JOIN events B ON A.job_id=B.job_id AND B.event_function='RESTART_CHECKING_AFTER' AND (B.event_option=0 OR B.current_option<B.event_option)")
                            .restartTask(TaskStatus.CHECKING_LIMIT)
                }
            }
        }
        else if (status == TaskStatus.READY) {
            //onTaskReady events
            dh.openQross()
                .get(s"SELECT job_id, event_name, event_function, event_value FROM qross_jobs_events WHERE job_id=$jobId AND enabled='yes' AND event_name='onTaskReady'")
            if (dh.nonEmpty) {
                dh.cache("events")
                    .get(s"""SELECT B.task_id, A.title, A.owner, B.job_id, B.task_time
                            FROM (SELECT id, title, owner FROM qross_jobs WHERE id=$jobId) A
                            INNER JOIN (SELECT id AS task_id,  job_id, task_time, record_time FROM qross_tasks WHERE id=$taskId) B ON A.id=B.job_id""")
                        .cache("task_info")
                dh.openCache()
                    .get(s"SELECT A.task_id, A.job_id, A.title, A.owner, A.task_time, A.record_time, B.event_value AS receivers, '' AS event_value, B.event_name, B.event_function FROM task_info A INNER JOIN events B ON A.job_id=B.job_id AND A.status='${TaskStatus.READY}' AND B.event_name='onTaskReady' AND B.event_function='SEND_MAIL_TO'")
                        .sendEmail(TaskStatus.READY)
                    .get(s"SELECT A.task_id, A.job_id, A.title, A.owner, A.task_time, A.record_time, B.event_value AS api, '' AS event_value, B.event_name, B.event_function FROM task_info A INNER JOIN events B ON A.job_id=B.job_id AND A.status='${TaskStatus.READY}' AND B.event_name='onTaskReady' AND B.event_function='REQUEST_API'")
                        .requestApi(TaskStatus.READY)
            }
        }

        dh.close()

        TaskRecorder.of(jobId, taskId, recordTime).log(s"Task $taskId of job $jobId at <$recordTime> status is ${if (status == TaskStatus.INITIALIZED) "not ready" else status} after pre-dependencies checking.")

        status == TaskStatus.READY
    }

    def checkTasksStatus(tick: String): Unit = {
        val dh = DataHub.Qross

        //tasks to be restart
        dh.get(s"SELECT id AS task_id, status FROM qross_tasks WHERE to_be_start_time='$tick'")
        if (dh.nonEmpty) {
            dh.cache("tasks")
            dh.openCache()
                .get(s"SELECT task_id FROM tasks WHERE status='${TaskStatus.CHECKING_LIMIT}'")
                    .put(s"UPDATE qross_tasks SET retry_times=0, status='${TaskStatus.INITIALIZED}' WHERE id=#task_id")
                .get(s"SELECT task_id FROM tasks WHERE status='${TaskStatus.INCORRECT}'")
                    .put("INSERT INTO qross_message_box (message_type, message_key, message_text) VALUES ('TASK', 'RESTART', '^WHOLE@#task_id')")
                .get(s"SELECT task_id FROM tasks WHERE status='${TaskStatus.FAILED}' OR status='${TaskStatus.TIMEOUT}'")
                    .put("INSERT INTO qross_message_box (message_type, message_key, message_text) VALUES ('TASK', 'RESTART', '^EXCEPTIONAL@#task_id')")
        }
        dh.clear()

        //stuck tasks - executing tasks but no running actions
        //get executing tasks
        dh.get(
            s"""select id, job_id, task_id, command_id, upstream_ids, record_time, TIMESTAMPDIFF(SECOND, update_time, NOW()) AS span, status
               | FROM qross_tasks_dags WHERE task_id IN (SELECT id FROM qross_tasks WHERE status='${TaskStatus.EXECUTING}')""".stripMargin)
                .cache("dags")
        dh.openCache()
                //check done and waiting actions only
            //.get(s"SELECT DISTINCT task_id FROM dags WHERE status NOT IN ('${ActionStatus.DONE}', '${ActionStatus.WAITING}')")
                //delete all exceptional actions
            .set(s"DELETE FROM dags WHERE task_id IN (SELECT DISTINCT task_id FROM dags WHERE status NOT IN ('${ActionStatus.DONE}', '${ActionStatus.WAITING}'))")
                //delete all non empty actions
            .set("DELETE FROM dags WHERE upstream_ids<>''")
                //check if stuck or not (waiting beyond 5 min)
            .get("SELECT job_id, task_id, record_time, MIN(span) AS span FROM dags GROUP BY job_id, task_id, record_time HAVING MIN(span)>600")
                //add or update stuck record
                .put("INSERT INTO qross_stuck_records (job_id, task_id, record_time) VALUES (#job_id, #task_id, '#record_time') ON DUPLICATE KEY UPDATE check_times=check_times+1")

        dh.openQross()
        if (dh.nonEmpty) {
            //reset status to READY if reach 3 times
            dh.get("SELECT id, task_id FROM qross_stuck_records WHERE check_times>=3 AND reseted='no'")
                .put(s"UPDATE qross_tasks SET status='${TaskStatus.READY}' WHERE id=#task_id AND status='${TaskStatus.EXECUTING}'")
                .put(s"UPDATE qross_stuck_records SET reseted='yes' where id=#id")
        }

        writeMessage("TaskStarter beat!")
        dh.set(s"UPDATE qross_keeper_beats SET last_beat_time=NOW() WHERE actor_name='TaskStarter'")

        dh.close()
    }

    //TaskStarter - execute()
    def getTaskCommandsToExecute(task: Task): DataTable = synchronized {

        val ds = new DataSource()

        val taskId = task.id
        val jobId = task.jobId
        val status = task.status
        val recordTime = task.recordTime
        val concurrentLimit = ds.executeSingleValue(s"SELECT concurrent_limit FROM qross_jobs WHERE id=$jobId").asInteger(1)

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
                    TaskRecorder.of(jobId, taskId, recordTime).warn(s"Task $taskId of job $jobId at <$recordTime> has been  closed because no commands exists on task ready.")
                }
                else if (map.containsKey(ActionStatus.EXCEPTIONAL) || map.containsKey(ActionStatus.OVERTIME)) {
                    //restart task if exceptional or overtime
                    ds.executeNonQuery(s"INSERT INTO qross_message_box (message_type, message_key, message_text) VALUES ('TASK', 'RESTART', '^EXCEPTIONAL@$taskId')")
                    TaskRecorder.of(jobId, taskId, recordTime).warn(s"Task $taskId of job $jobId at <$recordTime> restart because EXCEPTIONAL commands exists on task ready.")
                }
                else if (map.containsKey(ActionStatus.WAITING) || map.containsKey(ActionStatus.QUEUEING) || map.containsKey(ActionStatus.RUNNING)) {
                    //executing
                    ds.executeNonQuery(s"UPDATE qross_tasks SET status='${TaskStatus.EXECUTING}', start_time=NOW() WHERE id=$taskId")
                    TaskRecorder.of(jobId, taskId, recordTime).log(s"Task $taskId of job $jobId at <$recordTime> start executing on task ready.")
                }
                else {
                    //finished
                    ds.executeNonQuery(s"UPDATE qross_tasks SET status='${TaskStatus.FINISHED}' WHERE id=$taskId")
                    TaskRecorder.of(jobId, taskId, recordTime).warn(s"Task $taskId of job $jobId at <$recordTime> changes status to '${TaskStatus.FINISHED}' because all commands has been executed on task ready.")
                }
            }
            else {
                TaskRecorder.of(jobId, taskId, recordTime).warn(s"Concurrent reach upper limit of Job $jobId for Task $taskId at <$recordTime> on task ready.")
            }
        }

        val executable = ds.executeDataTable(
            s"""SELECT A.action_id, A.job_id, A.task_id, A.command_id, B.task_time, B.record_time, C.command_type, A.command_text, C.overtime, C.retry_limit, D.title, D.owner
                         FROM (SELECT id AS action_id, job_id, task_id, command_id, command_text FROM qross_tasks_dags WHERE task_id=$taskId AND record_time='$recordTime' AND status='${ActionStatus.WAITING}' AND upstream_ids='') A
                         INNER JOIN (SELECT id, task_time, record_time FROM qross_tasks WHERE id=$taskId AND status='${TaskStatus.EXECUTING}') B ON A.task_id=B.id
                         INNER JOIN (SELECT id, command_type, overtime, retry_limit FROM qross_jobs_dags WHERE job_id=$jobId) C ON A.command_id=C.id
                         INNER JOIN (SELECT id, title, owner FROM qross_jobs WHERE id=$jobId) D ON A.job_id=D.id""")

        writeDebugging(s"Task $taskId of $jobId get ${executable.count()} commands to execute.")

        //prepare to run command - start time point
        ds.tableUpdate(s"UPDATE qross_tasks_dags SET start_time=NOW(), status='${ActionStatus.QUEUEING}' WHERE id=#action_id", executable)

        ds.close()

        executable
    }

    //TaskExecutor
    def executeTaskCommand(taskCommand: DataRow): Task = {

        val dh = DataHub.Qross

        val jobId = taskCommand.getInt("job_id")
        val taskId = taskCommand.getLong("task_id")
        val commandId = taskCommand.getInt("command_id")
        val actionId = taskCommand.getLong("action_id")
        val taskTime = taskCommand.getString("task_time")
        val retryLimit = taskCommand.getInt("retry_limit")
        val overtime = taskCommand.getInt("overtime")
        val recordTime = taskCommand.getString("record_time")
        val commandType = taskCommand.getString("command_type")

        var commandText = taskCommand.getString("command_text")
                                     .replace("#{jobId}", s"$jobId")
                                     .replace("#{taskId}", s"$taskId")
                                     .replace("#{taskTime}", taskTime)

        if (commandType == "shell") {
            commandText = commandText.replace("%JAVA_BIN_HOME", Global.JAVA_BIN_HOME)
                                     .replace("%QROSS_HOME", Global.QROSS_HOME)
                                     .$restore(new PSQL(commandText, dh), "") //按PSQL计算, 支持各种PSQL嵌入式表达式, 但不保留引号

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
        }

        dh.set(s"UPDATE qross_tasks_dags SET status='${ActionStatus.RUNNING}', command_text=?, run_time=NOW(), waiting=TIMESTAMPDIFF(SECOND, start_time, NOW()) WHERE id=$actionId", commandText)

        if (commandType.endsWith("sql")) {
            commandText = Global.JAVA_BIN_HOME + s"java -jar ${Global.QROSS_HOME}qross-worker-${Global.QROSS_VERSION}.jar --task $actionId"
        }

        var retry = -1
        var exitValue = 1
        var next = false
        var killed = false

        //LET's GO!
        val logger = TaskRecorder.of(jobId, taskId, recordTime).run(commandId, actionId)
        logger.debug(s"START action $actionId - command $commandId of task $taskId - job $jobId at <$recordTime>: $commandText")

        do {
            if (retry > 0) {
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
                        logger.warn(s"Action $actionId - command $commandId of task $taskId - job $jobId at <$recordTime> is TIMEOUT: $commandText")
                    }
                    else if (TO_BE_KILLED.contains(actionId)) {
                        TO_BE_KILLED -= actionId
                        process.destroy() //kill it
                        killed = true
                        logger.warn(s"Action $actionId - command $commandId of task $taskId - job $jobId at <$recordTime> has been KILLED: $commandText")
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

            if (timeout) {
                exitValue = -1
            }
            else if (killed) {
                exitValue = -2
            }

            retry += 1
        }
        while (retry < retryLimit && exitValue != 0 && !killed)

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

                    dh.get(s"SELECT COUNT(0) AS amount FROM qross_tasks_dependencies WHERE task_id=$taskId AND record_time='$recordTime' AND dependency_moment='after' AND ready='no'")
                        .insertRowIfEmpty("amount" -> 0)
                        .put(s"UPDATE qross_tasks SET status=IF(#amount > 0, '${TaskStatus.INCORRECT}', '${TaskStatus.SUCCESS}'), checked=IF(#amount > 0, 'no', '') WHERE id=$taskId AND status='${TaskStatus.FINISHED}'")
                }
            //timeout
            case -1 =>
                dh.set(s"UPDATE qross_tasks_dags SET status='${ActionStatus.OVERTIME}', retry_times=$retry WHERE id=$actionId")
                dh.set(s"UPDATE qross_tasks SET status='${TaskStatus.TIMEOUT}', checked='no' WHERE id=$taskId")
            //killed
            case -2 =>
                dh.set(s"UPDATE qross_tasks_dags SET status='${ActionStatus.KILLED}' WHERE id=$actionId")
                dh.set(s"UPDATE qross_tasks SET status='${TaskStatus.INTERRUPTED}' WHERE id=$taskId")
            //failed
            case _ =>
                dh.set(s"UPDATE qross_tasks_dags SET status='${ActionStatus.EXCEPTIONAL}', retry_times=$retry WHERE id=$actionId")
                dh.set(s"UPDATE qross_tasks SET finish_time=NOW(), status='${TaskStatus.FAILED}', checked='no' WHERE id=$taskId")
        }

        val status = dh.executeSingleValue(s"SELECT status FROM qross_tasks WHERE id=$taskId").asText("miss")
        //send notification mail if failed or timeout or incorrect
        if (status == TaskStatus.SUCCESS || status == TaskStatus.FAILED || status == TaskStatus.TIMEOUT || status == TaskStatus.INCORRECT) {

            dh.get(s"""SELECT A.job_id, A.event_name, A.event_function, A.event_value, A.event_option, IFNULL(B.current_option, 0) AS current_option FROM
                    (SELECT job_id, event_name, event_function, event_value, event_option FROM qross_jobs_events WHERE job_id=$jobId AND event_name='onTask${status.capitalize}' AND enabled='yes') A
                        LEFT JOIN
                    (SELECT job_id, event_function, COUNT(0) AS current_option FROM qross_tasks_events WHERE task_id=$taskId AND event_name='onTask${status.capitalize}' AND archived=0 GROUP BY job_id, event_function) B
                    ON A.job_id=B.job_id AND A.event_function=B.event_function""")
            if (dh.nonEmpty) {
                dh.cache("events")
                dh.cache("task_info", DataTable(taskCommand))

                dh.get(s"SELECT CAST(create_time AS CHAR) AS create_time, log_type, log_text FROM qross_tasks_logs WHERE task_id=$taskId AND action_id=$actionId ORDER BY create_time ASC")
                    .buffer("logs")

                dh.openCache()
                    .get("SELECT A.*, B.event_value AS receivers, '' AS event_value, B.event_name, B.event_function FROM task_info A INNER JOIN events B ON A.job_id=B.job_id AND event_function='SEND_MAIL_TO'")
                        .sendEmail(status)
                    .get("SELECT A.*, B.event_value AS api, '' AS event_value, B.event_name, B.event_function FROM task_info A INNER JOIN events B ON A.job_id=B.job_id AND B.event_function='REQUEST_API'")
                        .requestApi(status)
                    .get(s"SELECT A.*, B.event_value AS delay, '' AS to_be_start_time, '' AS event_value, B.event_name, B.event_function FROM task_info A INNER JOIN events B WHERE B.event_function='RESTART_TASK' AND (B.event_option=0 OR B.current_option<B.event_option)") //SIGNED is MySQL syntax, but SQLite will ignore it.
                        .restartTask(status)
            }
        }

        dh.close()

        if (next) {
            //return
            Task(taskId, TaskStatus.EXECUTING).of(jobId).at(taskTime, recordTime)
        } else {
            //record and clear logger
            TaskRecorder.of(jobId, taskId, recordTime).debug(s"Task $taskId of job $jobId at <$recordTime> finish with status ${status.toUpperCase}.").dispose()
            //return nothing
            Task(0)
        }
    }
}