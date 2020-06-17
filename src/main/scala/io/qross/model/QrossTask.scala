package io.qross.model

import io.qross.core.{DataHub, DataRow, DataTable, DataType}
import io.qross.ext.Output._
import io.qross.ext.TypeExt._
import io.qross.fs.ResourceFile
import io.qross.jdbc.DataSource
import io.qross.keeper.Setting
import io.qross.net.Json
import io.qross.pql.Patterns.$BLANK
import io.qross.pql.PQL
import io.qross.pql.Solver._
import io.qross.setting.Global
import io.qross.time.{ChronExp, DateTime, Timer}
import io.qross.time.TimeSpan._

import scala.collection.mutable
import scala.sys.process._

object QrossTask {

    //
    val TO_BE_KILLED: mutable.HashMap[Long, Int] = new mutable.HashMap[Long, Int]()

    implicit class DataHub$Task(dh: DataHub) {

        def sendEmail(taskStatus: String): DataHub = {

            val upperStatus = taskStatus.toUpperCase()
            dh.foreach(row => {
                val receivers = row.getString("receivers")
                if (receivers != "") {
                    try {
                        row.set("event_result",
                            ResourceFile.open(s"/templates/$taskStatus.html")
                                .replace("#{status}", upperStatus)
                                .replaceWith(row)
                                //.replace("#{logs}", if (dh.containsBuffer("logs")) TaskRecorder.toHTML(dh.takeOut("logs")) else "")
                                .writeEmail(s"$upperStatus: ${row.getString("title")} ${row.getString("task_time")} - JobID: ${row.getString("job_id")} - TaskID: ${row.getString("task_id")}")
                                .to(if (receivers.contains("_OWNER")) row.getString("owner") else "")
                                .cc(if (receivers.contains("_KEEPER")) Setting.KEEPER_USER_GROUP else "")
                                .bcc(if (receivers.contains("_MASTER")) Setting.MASTER_USER_GROUP else "")
                                .send())

                        TaskRecorder.of(row.getInt("job_id"), row.getLong("task_id"), row.getString("record_time"))
                                .debug(s"Task ${row.getLong("task_id")} of job ${row.getInt("job_id")} at <${row.getString("record_time")}> sent a mail on task $taskStatus")
                    }
                    catch {
                        case e: Exception => row.set("event_result", e.getMessage)
                    }
                }
                else {
                    row.set("event_result", "NO RECEIVERS")
                }
            }).put("INSERT INTO qross_tasks_events (job_id, task_id, record_time, event_name, event_function, event_limit, event_value, event_result) VALUES (#job_id, #task_id, '#record_time', '#event_name', '#event_function', '#event_limit', '#receivers', '#event_result')")

            dh.clear()
        }

        def requestApi(taskStatus: String): DataHub = {
            dh.foreach(row => {
                val api = row.getString("api")
                val method = row.getString("method", "GET")

                if (api != "") {
                    val result = try {
                            Json.fromURL(api.replaceArguments(row).$restore(new PQL("", DataHub.DEFAULT).set(row), ""), method).toString()
                        }
                        catch {
                            case e: Exception => e.getMessage
                            case _: Throwable => ""
                        }

                    TaskRecorder.of(row.getInt("job_id"), row.getLong("task_id"), row.getString("record_time"))
                            .debug(s"Task ${row.getLong("task_id")} of job ${row.getInt("job_id")} at <${row.getString("record_time")}> requested api on task $taskStatus, result is $result")

                    row.set("event_result", result)
                }
                else {
                    row.set("event_result", "EMPTY API")
                }
            }).put("INSERT INTO qross_tasks_events (job_id, task_id, record_time, event_name, event_function, event_limit, event_value, event_result) VALUES (#job_id, #task_id, '#record_time', '#event_name', '#event_function', '#event_limit', '#api', '#event_result')")
                .clear()
        }

        def fireCustomEvent(taskStatus: String): DataHub = {
            dh.foreach(row => {
                val custom = row.getString("event_function").takeAfter("_")
                val SQL = dh.executeSingleValue("SELECT event_logic FROM qross_keeper_custom_events WHERE id=" + custom).asText("").replaceArguments(row)
                //execute
                try {
                    row.set("event_result", new PQL(SQL, DataHub.QROSS).set(row).run().toString)
                }
                catch {
                    case e: Exception =>
                        row.set("event_result", e.getMessage)
                        e.printStackTrace()
                }

                TaskRecorder.of(row.getInt("job_id"), row.getLong("task_id"), row.getString("record_time"))
                    .debug(s"Task ${row.getLong("task_id")} of job ${row.getInt("job_id")} at <${row.getString("record_time")}> fire custom event $custom on task $taskStatus.")

            }).put("INSERT INTO qross_tasks_events (job_id, task_id, record_time, event_name, event_function, event_limit, event_value, event_result) VALUES (#job_id, #task_id, '#record_time', '#event_name', '#event_function', '#event_limit', '#api', '#event_result')")
                .clear()
        }

        def runPQL(taskStatus: String): DataHub = {
            dh.foreach(row => {
                val SQL = row.getString("pql").replaceArguments(row)

                //execute
                try {
                    row.set("event_result", new PQL(SQL, DataHub.DEFAULT).set(row).run().toString)
                }
                catch {
                    case e: Exception =>
                        row.set("event_result", e.getMessage)
                        e.printStackTrace()
                }

                TaskRecorder.of(row.getInt("job_id"), row.getLong("task_id"), row.getString("record_time"))
                        .debug(s"Task ${row.getLong("task_id")} of job ${row.getInt("job_id")} at <${row.getString("record_time")}> execute PQL on task $taskStatus.")
            }).put("INSERT INTO qross_tasks_events (job_id, task_id, record_time, event_name, event_function, event_limit, event_value, event_result) VALUES (#job_id, #task_id, '#record_time', '#event_name', '#event_function', '#event_limit', '#pql', '#event_result')")
                    .clear()
        }

        def restartTask(taskStatus: String): DataHub = {

            if (dh.nonEmpty) {
                dh.put("INSERT INTO qross_tasks_events (job_id, task_id, record_time, event_name, event_function, event_limit, event_value, event_result) VALUES (#job_id, #task_id, '#record_time', '#event_name', '#event_function', '#event_limit', '#delay', 'RESTARTED.')")
                .foreach(row => {
                    val delay = row.getInt("delay", 30)
                    row.set("to_be_start_time", DateTime.now.plusMinutes(delay).getString("yyyyMMddHHmm00"))

                    TaskRecorder.of(row.getInt("job_id"), row.getLong("task_id"), row.getString("record_time")).debug(s"Task ${row.getLong("task_id")} of job ${row.getInt("job_id")} at <${row.getString("record_time")}> will restart after $delay minutes by $taskStatus.").dispose()
                })
                .put("UPDATE qross_tasks SET to_be_start_time='#to_be_start_time' WHERE id=#task_id")
            }

            dh.clear()
        }


        def generateDependencies(): DataHub = {

            if (dh.nonEmpty) {
                val PQL = new PQL("", DataHub.DEFAULT)

                dh.foreach(row => {
                    PQL.set(row)
                    try {
                        row.set("dependency_content",
                            row.getString("dependency_content")
                              .replaceArguments(row)
                              .$restore(PQL, "\""), DataType.TEXT)
                      row.set("dependency_option",
                          row.getString("dependency_option")
                            .replaceArguments(row)
                            .$restore(PQL, "\""), DataType.TEXT)
                    }
                    catch {
                        case e: Exception =>
                            TaskRecorder.of(row.getInt("job_id"), row.getLong("task_id"), row.getString("record_time"))
                                    .err("Wrong dependency value of configuration.")
                                    .err(e.getMessage)

                    }
                }).put("INSERT INTO qross_tasks_dependencies (job_id, task_id, record_time, dependency_id, dependency_moment, dependency_type, dependency_label, dependency_content, dependency_option) VALUES (#job_id, #task_id, '#record_time', #dependency_id, '#dependency_moment', '#dependency_type', '#dependency_label', '#dependency_content', '#dependency_option')")
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

        val dh = DataHub.QROSS

        //get last tick of producer
        val lastBeat = dh.executeSingleValue("SELECT last_beat_time FROM qross_keeper_beats WHERE actor_name='TaskProducer'")
                            .asDateTimeOrElse(DateTime.now)
                            .setSecond(0)
                            .setNano(0)
                            .plusMinutes(1)
        val nextBeat = DateTime.now.setSecond(0).setNano(0).plusMinutes(1)

        //get all jobs
        dh.openQross()
            //next_tick != '' means this is not a new job
            .get(s"SELECT id AS job_id, cron_exp, next_tick, complement_missed_tasks FROM qross_jobs WHERE job_type='${JobType.SCHEDULED}' AND enabled='yes' AND next_tick<>'' AND next_tick<>'NONE' AND next_tick<${nextBeat.getTickValue}")
                .cache("jobs")

        //get all ticks for jobs that need to complement during server was offline
        dh.openCache()
            .get("SELECT job_id, cron_exp, next_tick FROM jobs WHERE complement_missed_tasks='yes'")
                .table("job_id" -> DataType.INTEGER, "next_tick" -> DataType.TEXT) (row => {
                    val table = new DataTable()
                    val jobId = row.getInt("job_id")
                    val ticks = try {
                        ChronExp.getTicks(row.getString("cron_exp"), lastBeat.getTickValue, nextBeat.getTickValue)
                    }
                    catch {
                        case e: Exception =>
                            writeException(e.getMessage)
                            List[String]()
                    }
                    ticks.foreach(time => {
                        table.insert(
                            "job_id" -> jobId,
                            "next_tick" -> time
                        )
                    })
                    table
                }).cache("missed_tasks")

        //get exists tasks during offline
        dh.openQross()
            .get(s"SELECT job_id, task_time FROM qross_tasks WHERE job_id IN (SELECT id FROM qross_jobs WHERE job_type='${JobType.SCHEDULED}' AND enabled='yes' AND next_tick<>'' AND next_tick<>'NONE' AND complement_missed_tasks='yes') AND task_time>${lastBeat.getTickValue} AND task_time<${nextBeat.getTickValue}")
                .cache("exists_tasks")

        //complement all jobs
        dh.openCache()
            .get(s"SELECT A.job_id, A.next_tick FROM missed_tasks A LEFT JOIN exists_tasks B ON A.job_id=B.job_id AND A.next_tick=B.task_time WHERE B.job_id IS NULL")
                .put(s"INSERT INTO qross_tasks (job_id, task_time, record_time, create_mode, start_mode) VALUES (?, ?, '${DateTime.now.toString}', 'complement', 'auto_start')")

        //get next tick for all jobs
        dh.openCache()
            .get("SELECT job_id, cron_exp, next_tick FROM jobs")
                .foreach(row => {
                    try {
                        val cron = row.getString("cron_exp")
                        if (cron != "") {
                            row.set("next_tick", new ChronExp(cron).getNextTickOrNone(nextBeat))
                        }
                        else {
                            row.set("next_tick", "")
                        }
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
        val minute = new DateTime(tick)

        val dh = DataHub.QROSS

        //update empty next_tick - it will be empty when create a new job
        //update outdated jobs - it will occur when you enable a job from disabled
        dh
            //next_tick will be NONE if cron exp is expired.
            .get(s"SELECT id AS job_id, cron_exp, '' AS next_tick FROM qross_jobs WHERE job_type='${JobType.SCHEDULED}' AND enabled='yes' AND (next_tick='' OR (next_tick<>'NONE' AND next_tick<$tick))")
                .foreach(row =>
                    try {
                        row.set("next_tick", ChronExp(row.getString("cron_exp")).getNextTickOrNone(minute))
                    }
                    catch {
                        case e: Exception => writeException(e.getMessage)
                    }
                ).put("UPDATE qross_jobs SET next_tick='#next_tick' WHERE id=#job_id")

        //create tasks without cron_exp
        //excluding jobs with executing tasks
        dh.get(s"""SELECT id AS job_id FROM qross_jobs WHERE job_type='${JobType.DEPENDENT}' AND enabled='yes' AND id NOT IN (SELECT DISTINCT job_id FROM qross_tasks WHERE
                                    status NOT IN ('${TaskStatus.FINISHED}', '${TaskStatus.INCORRECT}', '${TaskStatus.FAILED}', '${TaskStatus.TIMEOUT}', '${TaskStatus.SUCCESS}'))""")
            .put(s"INSERT INTO qross_tasks (job_id, task_time, record_time, create_mode, start_mode) VALUES (#job_id, '${DateTime.now.getString("yyyyMMddHHmmss")}', '${DateTime.now}', 'trigger', 'auto_start')")

        //jobs with cron_exp
        dh.get(s"SELECT id AS job_id, cron_exp, next_tick FROM qross_jobs WHERE next_tick='$tick' AND job_type='${JobType.SCHEDULED}' AND enabled='yes' AND id NOT IN (SELECT job_id FROM qross_tasks WHERE task_time='$tick')")
            //create schedule tasks
                .put(s"INSERT INTO qross_tasks (job_id, task_time, record_time, create_mode, start_mode) VALUES (#job_id, '#next_tick', '${DateTime.now.toString}', 'schedule', 'auto_start')")
            //get next tick and update
            .foreach(row => {
                try {
                    row.set("next_tick", ChronExp(row.getString("cron_exp")).getNextTickOrNone(minute.plusMinutes(1))) //get next minute to match next tick
                }
                catch {
                    case e: Exception =>  writeException(e.getMessage)
                }
            }).put("UPDATE qross_jobs SET next_tick='#next_tick' WHERE id=#job_id")

        //get all new tasks
        dh.get(
                s"""SELECT A.task_id, A.job_id, C.title, C.owner, A.task_time, A.record_time, A.start_mode, IFNULL(B.dependencies, 0) AS dependencies
                   FROM (SELECT id AS task_id, job_id, task_time, record_time, start_mode FROM qross_tasks WHERE status='${TaskStatus.NEW}') A
                   INNER JOIN qross_jobs C ON A.job_id=C.id
                   LEFT JOIN (SELECT job_id, COUNT(0) AS dependencies FROM qross_jobs_dependencies WHERE dependency_moment='before' GROUP BY job_id) B ON A.job_id=B.job_id""")
                .cache("tasks")

        if (dh.nonEmpty) {
            //onTaskNew events
            dh.openCache()
                //update status
                .get("SELECT GROUP_CONCAT(job_id) AS job_ids FROM tasks")
            .openQross()
                .pass("SELECT job_id, event_name, event_function, event_limit, event_value, event_option FROM qross_jobs_events FORCE INDEX (idx_qross_jobs_events_select) WHERE job_id IN (#job_ids) AND enabled='yes' AND event_name='onTaskNew'")
            if (dh.nonEmpty) {
                dh.cache("events")
                //onTaskNew Event
                dh.openCache()
                    .get("SELECT A.task_id, A.job_id, A.title, A.owner, A.task_time, A.record_time, B.event_value AS receivers, B.event_name, B.event_function, B.event_limit, '' AS event_result FROM tasks A INNER JOIN events B ON A.job_id=B.job_id AND B.event_function='SEND_MAIL_TO' AND INSTR(B.event_limit, A.start_mode)>0")
                        .sendEmail(TaskStatus.NEW)
                    .get("SELECT A.task_id, A.job_id, A.title, A.owner, A.task_time, A.record_time, B.event_value AS api, B.event_option AS method, B.event_name, B.event_function, B.event_limit, '' AS event_result FROM tasks A INNER JOIN events B ON A.job_id=B.job_id AND B.event_function='REQUEST_API' AND INSTR(B.event_limit, A.start_mode)>0")
                        .requestApi(TaskStatus.NEW)
                    .get("SELECT A.task_id, A.job_id, A.title, A.owner, A.task_time, A.record_time, B.event_value AS roles, B.event_name, B.event_function, B.event_limit, '' AS event_result FROM tasks A INNER JOIN events B ON A.job_id=B.job_id AND INSTR(B.event_function, 'CUSTOM_')>0 AND INSTR(B.event_limit, A.start_mode)>0")
                        .fireCustomEvent(TaskStatus.NEW)
                    .get("SELECT A.task_id, A.job_id, A.title, A.owner, A.task_time, A.record_time, B.event_value AS pql, B.event_name, B.event_function, B.event_limit, '' AS event_result FROM tasks A INNER JOIN events B ON A.job_id=B.job_id AND B.event_function='EXECUTE_PQL' AND INSTR(B.event_limit, A.start_mode)>0")
                        .runPQL(TaskStatus.NEW)
            }
            dh.clear()

            // ----- dependencies -----

            //get all dependencies
            dh.openCache()
                .get("SELECT IFNULL(GROUP_CONCAT(DISTINCT job_id), 0) AS job_ids FROM tasks WHERE dependencies>0")
            dh.openQross()
                .pass("SELECT id AS dependency_id, job_id, dependency_moment, dependency_type, dependency_label, dependency_content, dependency_option FROM qross_jobs_dependencies WHERE job_id IN (#job_ids)")
                    .cache("dependencies")

            //generate dependencies
            dh.openCache()
                .get("SELECT A.job_id, A.task_id, A.task_time, A.record_time, B.dependency_id, B.dependency_moment, B.dependency_type, B.dependency_label, B.dependency_content, B.dependency_option FROM tasks A INNER JOIN dependencies B ON A.job_id=B.job_id")
                    .generateDependencies()

            // ---------- DAGs ----------

            //get all DAGs
            dh.openCache()
                .get("SELECT IFNULL(GROUP_CONCAT(DISTINCT job_id), 0) AS job_ids FROM tasks")
            dh.openQross()
                .pass("SELECT id AS command_id, command_text, job_id, upstream_ids FROM qross_jobs_dags WHERE job_id IN (#job_ids) AND enabled='yes'")
                    .cache("dags")

            //generate DAGs
            dh.openCache()
                .get("SELECT A.job_id, A.task_id, A.record_time, B.command_id, B.command_text, B.upstream_ids FROM tasks A INNER JOIN dags B ON A.job_id=B.job_id")
                    .put("INSERT INTO qross_tasks_dags (job_id, task_id, record_time, command_id, command_text, upstream_ids) VALUES (?, ?, ?, ?, ?, ?)")

            //update tasks status
            dh.get(s"SELECT task_id FROM tasks WHERE dependencies>0")
                .put(s"UPDATE qross_tasks SET status='${TaskStatus.INITIALIZED}'")
            .get("SELECT task_id FROM tasks WHERE dependencies=0")
                .put("UPDATE qross_tasks SET status='${TaskStatus.READY}',ready_time=NOW(), readiness=TIMESTAMP(SECOND, create_time, NOW()) WHERE id=#task_id")

            //Master will can't turn on job if no commands to execute - 2018.9.8
            dh.get("SELECT A.job_id, A.task_id FROM tasks A LEFT JOIN dags B ON A.job_id=B.job_id WHERE B.job_id IS NULL")
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

    def createEndlessTask(jobId: Int): Task = {

        val dh = DataHub.QROSS

        val recordTime = DateTime.now
        val taskTime = recordTime.getString("yyyyMMddHHmmss")

        dh.set(s"INSERT INTO qross_tasks (job_id, task_time, record_time, create_mode, start_mode) VALUES ($jobId, '$taskTime', '${recordTime.toString()}', 'interval', 'auto_start')")

        val taskId = dh.executeSingleValue(s"SELECT id FROM qross_tasks WHERE job_id=$jobId AND task_time='$taskTime'").asInteger

        dh.get(
            s"""SELECT A.task_id, A.job_id, B.title, B.owner, A.task_time, A.record_time, A.start_mode
                               FROM (SELECT id AS task_id, job_id, task_time, record_time, start_mode FROM qross_tasks WHERE id=$taskId) A
                               INNER JOIN qross_jobs B ON B.id=$jobId AND A.job_id=B.id""")
                .cache("task_info")

        dh.get(s"SELECT job_id, event_name, event_function, event_limit, event_value, event_option FROM qross_jobs_events WHERE job_id=$jobId AND enabled='yes' AND event_name='onTaskNew'")
        if (dh.nonEmpty) {
            dh.cache("events")
            //onTaskNew Event
            dh.openCache()
                .get("SELECT A.task_id, A.job_id, A.title, A.owner, A.task_time, A.record_time, B.event_value AS receivers, B.event_name, B.event_function, B.event_limit, '' AS event_result FROM task_info A INNER JOIN events B ON A.job_id=B.job_id AND B.event_function='SEND_MAIL_TO' AND INSTR(B.event_limit, A.start_mode)>0")
                    .sendEmail(TaskStatus.NEW)
                .get("SELECT A.task_id, A.job_id, A.title, A.owner, A.task_time, A.record_time, B.event_value AS api, B.event_option AS method, B.event_name, B.event_function, B.event_limit, '' AS event_result FROM task_info A INNER JOIN events B ON A.job_id=B.job_id AND event_function='REQUEST_API' AND INSTR(B.event_limit, A.start_mode)>0")
                    .requestApi(TaskStatus.NEW)
                .get("SELECT A.task_id, A.job_id, A.title, A.owner, A.task_time, A.record_time, B.event_value AS roles, B.event_name, B.event_function, B.event_limit, '' AS event_result FROM tasks A INNER JOIN events B ON A.job_id=B.job_id AND INSTR(B.event_function, 'CUSTOM_')>0 AND INSTR(B.event_limit, A.start_mode)>0")
                    .fireCustomEvent(TaskStatus.NEW)
                .get("SELECT A.task_id, A.job_id, A.title, A.owner, A.task_time, A.record_time, B.event_value AS pql, B.event_name, B.event_function, B.event_limit, '' AS event_result FROM task_info A INNER JOIN events B ON A.job_id=B.job_id AND event_function='EXECUTE_PQL' AND INSTR(B.event_limit, A.start_mode)>0")
                    .runPQL(TaskStatus.NEW)
        }
        dh.clear()

        // ---------- DAGs ----------
        dh.openQross()
            .get(s"SELECT id AS command_id, command_text, job_id, upstream_ids FROM qross_jobs_dags WHERE job_id=$jobId AND enabled='yes'")
                .cache("dags")
        //generate DAGs
        dh.openCache()
            .get("SELECT A.job_id, A.task_id, A.record_time, B.command_id, B.command_text, B.upstream_ids FROM task_info A INNER JOIN dags B ON A.job_id=B.job_id")
                .put("INSERT INTO qross_tasks_dags (job_id, task_id, record_time, command_id, command_text, upstream_ids) VALUES (?, ?, ?, ?, ?, ?)")

        //update tasks status
        dh.openQross()
            .set(s"UPDATE qross_tasks SET status='${TaskStatus.READY}', ready_time=NOW(), readiness=TIMESTAMP(SECOND, create_time, NOW()) WHERE id=$taskId")

        dh.close()

        Task(taskId, TaskStatus.READY).of(jobId).at(taskTime, recordTime.toString())
    }

    def createInstantTask(message: String, creator: Int): Option[Task] = {

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
        val params = info.getString("params").$split(",", ":") //to replace command text
        val commands = info.getString("commands").$split("##\\$##", ":")
        val delay = info.getInt("delay")
        val taskTime = DateTime.now.getString("yyyyMMddHHmmss")
        val recordTime = DateTime.now.toString()
        val startTime = info.getString("start_time")

        //maybe convert failure
        if (jobId > 0) {

            val dh = DataHub.QROSS

            //create task
            dh.set(s"INSERT INTO qross_tasks (job_id, task_time, record_time, status, creator, create_mode, start_mode, to_be_start_time) VALUES ($jobId, '$taskTime', '$recordTime', '${TaskStatus.INSTANT}', $creator, 'instant', 'manual_start', '$startTime')")

            //get task id
            dh.get(s"""SELECT A.task_id, A.job_id, A.task_time, A.record_time, A.start_mode, B.title, B.owner, IFNULL(C.dependencies, 0) AS dependencies
                        FROM (SELECT id AS task_id, job_id, task_time, record_time, start_mode FROM qross_tasks WHERE job_id=$jobId AND task_time='$taskTime' AND status='${TaskStatus.INSTANT}') A
                        INNER JOIN qross_jobs B ON A.job_id=B.id
                        LEFT JOIN (SELECT job_id, COUNT(0) AS dependencies FROM qross_jobs_dependencies WHERE job_id=$jobId AND dependency_moment='before' GROUP BY job_id) C ON A.job_id=C.job_id""")
                .cache("task_info")

            taskId = dh.firstRow.getLong("task_id")

            if (taskId > 0) {

                dh.get(s"SELECT job_id, event_name, event_function, event_limit, event_value, event_option FROM qross_jobs_events WHERE job_id=$jobId AND enabled='yes' AND event_name='onTaskNew'")

                if (dh.nonEmpty) {
                    dh.cache("events")
                    //onTaskNew event
                    dh.openCache()
                        .get("SELECT A.task_id, A.job_id, A.title, A.owner, A.task_time, A.record_time, B.event_value AS receivers, B.event_limit, '' AS event_result, B.event_name, B.event_function FROM task_info A INNER JOIN events B ON A.job_id=B.job_id AND B.event_function='SEND_MAIL_TO' AND INSTR(B.event_limit, A.start_mode)>0")
                            .sendEmail(TaskStatus.NEW)
                        .get("SELECT A.task_id, A.job_id, A.title, A.owner, A.task_time, A.record_time, B.event_value AS api, B.event_option AS method, B.event_limit, '' AS event_result, B.event_name, B.event_function FROM task_info A INNER JOIN events B ON A.job_id=B.job_id AND B.event_function='REQUEST_API' AND INSTR(B.event_limit, A.start_mode)>0")
                            .requestApi(TaskStatus.NEW)
                        .get("SELECT A.task_id, A.job_id, A.title, A.owner, A.task_time, A.record_time, B.event_value AS roles, B.event_name, B.event_function, B.event_limit, '' AS event_result FROM task_info A INNER JOIN events B ON A.job_id=B.job_id AND INSTR(B.event_function, 'CUSTOM_')>0 AND INSTR(B.event_limit, A.start_mode)>0")
                            .fireCustomEvent(TaskStatus.NEW)
                        .get("SELECT A.task_id, A.job_id, A.title, A.owner, A.task_time, A.record_time, B.event_value AS pql, B.event_limit, '' AS event_result, B.event_name, B.event_function FROM task_info A INNER JOIN events B ON A.job_id=B.job_id AND B.event_function='EXECUTE_PQL' AND INSTR(B.event_limit, A.start_mode)>0")
                            .runPQL(TaskStatus.NEW)
                }
                dh.clear()

                //dependencies
                dh.openQross()
                    .get(s"SELECT job_id, $taskId AS task_id, '$taskTime' AS task_time, '$recordTime' AS record_time, id AS dependency_id, dependency_moment, dependency_type, dependency_label, dependency_content, dependency_option FROM qross_jobs_dependencies WHERE job_id=$jobId")
                        .generateDependencies()

                //DAG
                dh.get(s"SELECT job_id, $taskId AS task_id, id AS command_id, command_text, upstream_ids FROM qross_jobs_dags WHERE job_id=$jobId AND enabled='yes'" + (if (dag != "") s" AND id IN ($dag)" else ""))
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
                    dh.get(s"SELECT id FROM qross_jobs_dags WHERE job_id=$jobId AND enabled='yes' AND id NOT IN ($dag)")
                        .put(s"UPDATE qross_tasks_dags SET upstream_ids=REPLACE(upstream_ids, '(#id)', '') WHERE task_id=$taskId")
                }

                //task status
                dh.openCache()
                    .get(s"SELECT job_id, (CASE WHEN dependencies>0 THEN '${TaskStatus.INITIALIZED}' ELSE '${TaskStatus.READY}' END) AS status FROM task_info")
                        .put(s"UPDATE qross_tasks SET status='#status' WHERE id=$taskId")

                status = dh.firstRow.getString("status")
            }

            //dh.openQross().set(s"INSERT INTO qross_query_result (query_id, result) VALUES ('$queryId', '$taskId')")

            dh.close()

            TaskRecorder.of(jobId, taskId, recordTime).debug(s"Instant Task $taskId of job $jobId at <$recordTime> has been created.")
        }

        if (delay > 0 && startTime == "" && jobId  > 0 && taskId > 0) {
            TaskRecorder.of(jobId, taskId, recordTime).debug(s"Instant Task $taskId of job $jobId at <$recordTime> will start after $delay seconds.")
            Timer.sleep({ if (delay > 60) 60 else delay } seconds)
        }

        if (startTime == "") {
            Some(Task(taskId, status).of(jobId).at(taskTime, recordTime))
        }
        else {
            None
        }
    }

    def restartTask(taskId: Long, option: String, starter: Int = 0): Task = {

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

        val dh = DataHub.QROSS

        //backup records
        dh.get(s"SELECT job_id, task_time, record_time, start_mode, starter, status, start_time, finish_time, readiness, latency, spent FROM qross_tasks WHERE id=$taskId")
            .put(s"INSERT INTO qross_tasks_records (job_id, task_id, record_time, start_mode, starter, status, start_time, finish_time, readiness, latency, spent) VALUES (#job_id, $taskId, '#record_time', '#start_mode', #starter, '#status', '#start_time', '#finish_time', #readiness, #latency, #spent)")

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
                    dh.get(s"""SELECT $jobId AS job_id, $taskId AS task_id, '$taskTime' AS task_time, '$recordTime' AS record_time, id AS dependency_id, dependency_moment, dependency_type, dependency_label, dependency_content, dependency_option FROM qross_jobs_dependencies WHERE job_id=$jobId""")
                        .cache("dependencies")
                        .generateDependencies()

                    //generate dags
                    dh.get(s"SELECT job_id, $taskId AS task_id, id AS command_id, command_text, upstream_ids FROM qross_jobs_dags WHERE job_id=$jobId AND enabled='yes'")
                        .put(s"INSERT INTO qross_tasks_dags (job_id, task_id, record_time, command_id, command_text, upstream_ids) VALUES (?, ?, '$recordTime', ?, ?, ?)")

                    dh.openCache().get("SELECT task_id FROM dependencies WHERE dependency_moment='before'")
                    if (dh.nonEmpty) {
                        status = TaskStatus.INITIALIZED
                    }

                case PARTIAL =>
                    dh.get(s"SELECT job_id, $taskId AS task_id, id AS command_id, command_text, upstream_ids FROM qross_jobs_dags WHERE job_id=$jobId AND enabled='yes' AND id IN ($option)")
                        .put(s"INSERT INTO qross_tasks_dags (job_id, task_id, record_time, command_id, command_text, upstream_ids) VALUES (?, ?, '$recordTime', ?, ?, ?)")
                    .get(s"SELECT id FROM qross_jobs_dags WHERE job_id=$jobId AND enabled='yes' AND id NOT IN ($option)")
                        .put(s"UPDATE qross_tasks_dags SET upstream_ids=REPLACE(upstream_ids, '(#id)', '') WHERE task_id=$taskId AND record_time='$recordTime")

                case EXCEPTIONAL =>
                    //generate "after" dependencies
                    dh.get(s"SELECT $jobId AS job_id, $taskId AS task_id, '$taskTime' AS task_time, '$recordTime' AS record_time, id AS dependency_id, dependency_moment, dependency_type, dependency_label, dependency_content, dependency_option FROM qross_jobs_dependencies WHERE job_id=$jobId AND dependency_moment='after'")
                        .generateDependencies()

                    dh.get(s"SELECT IFNULL(GROUP_CONCAT(command_id), 0) AS command_ids FROM qross_tasks_dags WHERE task_id=$taskId AND record_time='$prevRecordTime' AND status IN ('exceptional', 'overtime', 'waiting')")
                        .pass(s"SELECT job_id, $taskId AS task_id, id AS command_id, command_text, upstream_ids FROM qross_jobs_dags WHERE job_id=$jobId AND enabled='yes' AND id IN (#command_ids)")
                            .put(s"INSERT INTO qross_tasks_dags (job_id, task_id, record_time, command_id, command_text, upstream_ids) VALUES (?, ?, '$recordTime', ?, ?, ?)")
                        .get(s"SELECT command_id FROM qross_tasks_dags WHERE task_id=$taskId AND record_time='$prevRecordTime' AND status='done'")
                            .put(s"UPDATE qross_tasks_dags SET upstream_ids=REPLACE(upstream_ids, '(#command_id)', '') WHERE task_id=$taskId AND record_time='$recordTime'")
            }

            //update task
            dh.openQross()
                .set(s"UPDATE qross_tasks SET status='$status', start_mode='$restartMode', starter=$starter, record_time='$recordTime', readiness=NULL, latency=NULL, spent=NULL, start_time=NULL, finish_time=NULL WHERE id=$taskId")

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

        val dh = DataHub.QROSS

        //update task status to ready if all dependencies are ready
        dh.set(s"UPDATE qross_tasks SET status='${TaskStatus.READY}', ready_time=NOW(), readiness=TIMESTAMPDIFF(SECOND, create_time, NOW()) WHERE id=$taskId AND NOT EXISTS (SELECT task_id FROM qross_tasks_dependencies WHERE task_id=$taskId AND record_time='$recordTime' AND dependency_moment='before' AND ready='no')")

        //check dependencies
        dh.openQross()
            .get(
                s"""SELECT A.id, A.job_id, A.task_id, A.dependency_type, A.dependency_label, A.dependency_content, A.dependency_option, A.ready, B.task_time, A.record_time
                    FROM (SELECT id, job_id, task_id, record_time, dependency_type, dependency_label, dependency_content, dependency_option, ready FROM qross_tasks_dependencies WHERE task_id=$taskId AND record_time='$recordTime' AND dependency_moment='before' AND ready='no') A
                    INNER JOIN (SELECT id, task_time FROM qross_tasks where id=$taskId) B ON A.task_id=B.id""")
            .foreach(row => {
                if (TaskDependency.check(row) == "yes") {
                    row.set("ready", "yes")
                    TaskRecorder.of(jobId, taskId, recordTime).log(s"Task $taskId of job $jobId at <$recordTime> dependency ${row.getLong("id")} is ready.")
                }
            }).cache("dependencies")

        //update status and others after checking
        dh.openCache()
            .get("SELECT id FROM dependencies WHERE ready='yes'")
                .put("UPDATE qross_tasks_dependencies SET ready='yes' WHERE id=#id")
            .get("SELECT id FROM dependencies WHERE ready='no'")
                .put("UPDATE qross_tasks_dependencies SET retry_times=retry_times+1 WHERE id=?")
            .get("SELECT job_id, task_id FROM dependencies WHERE ready='no' GROUP BY task_id HAVING COUNT(0)=0")
                .put(s"UPDATE qross_tasks SET status='${TaskStatus.READY}', ready_time=NOW(), readiness=TIMESTAMP(SECOND, create_time, NOW()) WHERE id=#task_id")

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
                    .set(s"UPDATE qross_jobs SET unchecked_exceptional_tasks=IFNULL((SELECT GROUP_CONCAT(unchecked_exceptional_status ORDER BY unchecked_exceptional_status) AS exceptional_status FROM (SELECT CONCAT(`status`, ':', COUNT(0)) AS unchecked_exceptional_status FROM qross_tasks WHERE job_id=$jobId AND checked='no' GROUP BY `status`) T), '') WHERE id=$jobId;")

                TaskRecorder.of(jobId, taskId, recordTime).warn(s"Task $taskId of job $jobId at <$recordTime> reached upper limit of checking limit.").dispose()

                //update status if reach upper limit
                dh.join(s"""SELECT B.task_id, A.title, A.owner, B.job_id, B.task_time, B.record_time, B.start_mode
                            FROM (SELECT id, title, owner FROM qross_jobs WHERE id=$jobId) A
                            INNER JOIN (SELECT id AS task_id, job_id, task_time, record_time, start_mode FROM qross_tasks WHERE id=$taskId) B ON A.id=B.job_id""", "job_id" -> "job_id")
                        .cache("task_info")

                //execute onTaskCheckingLimit event
                dh.get(s"""SELECT A.job_id, A.event_name, A.event_function, A.event_limit, A.event_value, A.event_option, IFNULL(B.current_option, 0) AS current_option FROM
                        (SELECT job_id, event_name, event_function, event_limit, event_value, event_option FROM qross_jobs_events WHERE job_id=$jobId AND enabled='yes' AND event_name='onTaskCheckingLimit') A
                            LEFT JOIN
                        (SELECT job_id, event_function, COUNT(0) AS current_option FROM qross_tasks_events WHERE task_id=$taskId AND event_name='onTaskCheckingLimit' GROUP BY job_id, event_function) B
                        ON A.job_id=B.job_id AND A.event_function=B.event_function""")

                if (dh.nonEmpty) {
                    dh.cache("events")
                    //onTaskCheckingLimit
                    dh.openCache()
                        .get("SELECT A.*, B.event_value AS receivers, B.event_limit, '' AS event_result, B.event_name, B.event_function FROM task_info A INNER JOIN events B ON A.job_id=B.job_id WHERE event_function='SEND_MAIL_TO' AND INSTR(B.event_limit, A.start_mode)>0")
                            .sendEmail(TaskStatus.CHECKING_LIMIT)
                        .get("SELECT A.*, B.event_value AS api, B.event_option AS method, B.event_limit, '' AS event_result, B.event_name, B.event_function FROM task_info A INNER JOIN events B ON A.job_id=B.job_id AND B.event_function='REQUEST_API' AND INSTR(B.event_limit, A.start_mode)>0")
                            .requestApi(TaskStatus.CHECKING_LIMIT)
                        .get("SELECT A.*, B.event_value AS roles, B.event_function, B.event_limit, '' AS event_result, B.event_name FROM task_info A INNER JOIN events B ON A.job_id=B.job_id AND INSTR(B.event_function, 'CUSTOM_')>0 AND INSTR(B.event_limit, A.start_mode)>0")
                            .fireCustomEvent(TaskStatus.CHECKING_LIMIT)
                        .get("SELECT A.*, B.event_value AS delay, B.event_function, B.event_limit, '' AS to_be_start_time, B.event_name FROM task_info A INNER JOIN events B ON A.job_id=B.job_id AND B.event_function='RESTART_CHECKING_AFTER' AND (B.event_option=0 OR B.current_option<B.event_option) AND INSTR(B.event_limit, A.start_mode)>0")
                            .restartTask(TaskStatus.CHECKING_LIMIT)
                        .get("SELECT A.*, B.event_value AS pql, B.event_limit, '' AS event_result, B.event_name, B.event_function FROM task_info A INNER JOIN events B ON A.job_id=B.job_id AND B.event_function='EXECUTE_PQL' AND INSTR(B.event_limit, A.start_mode)>0")
                            .runPQL(TaskStatus.CHECKING_LIMIT)
                }
            }
        }
        else if (status == TaskStatus.READY) {
            //onTaskReady events
            dh.openQross()
                .get(s"SELECT job_id, event_name, event_function, event_limit, event_value, event_option FROM qross_jobs_events WHERE job_id=$jobId AND enabled='yes' AND event_name='onTaskReady'")
            if (dh.nonEmpty) {
                dh.cache("events")
                    .get(s"""SELECT B.task_id, A.title, A.owner, B.job_id, B.task_time, B.record_time, B.start_mode, B.status
                        FROM (SELECT id, title, owner FROM qross_jobs WHERE id=$jobId) A
                        INNER JOIN (SELECT id AS task_id, status, job_id, task_time, record_time, start_mode FROM qross_tasks WHERE id=$taskId) B ON A.id=B.job_id""")
                        .cache("task_info")
                dh.openCache()
                    .get(s"SELECT A.task_id, A.job_id, A.title, A.owner, A.task_time, A.record_time, B.event_value AS receivers, B.event_limit, '' AS event_result, B.event_name, B.event_function FROM task_info A INNER JOIN events B ON A.job_id=B.job_id AND B.event_function='SEND_MAIL_TO' AND INSTR(B.event_limit, A.start_mode)>0")
                        .sendEmail(TaskStatus.READY)
                    .get(s"SELECT A.task_id, A.job_id, A.title, A.owner, A.task_time, A.record_time, B.event_value AS api, B.event_option AS method, B.event_limit, '' AS event_result, B.event_name, B.event_function FROM task_info A INNER JOIN events B ON A.job_id=B.job_id AND B.event_function='REQUEST_API' AND INSTR(B.event_limit, A.start_mode)>0")
                        .requestApi(TaskStatus.READY)
                    .get(s"SELECT A.task_id, A.job_id, A.title, A.owner, A.task_time, A.record_time, B.event_value AS roles, B.event_function, B.event_limit, '' AS event_result, B.event_name FROM task_info A INNER JOIN events B ON A.job_id=B.job_id AND INSTR(B.event_function, 'CUSTOM_')>0 AND INSTR(B.event_limit, A.start_mode)>0")
                        .fireCustomEvent(TaskStatus.READY)
                    .get(s"SELECT A.task_id, A.job_id, A.title, A.owner, A.task_time, A.record_time, B.event_value AS pql, B.event_limit, '' AS event_result, B.event_name, B.event_function FROM task_info A INNER JOIN events B ON A.job_id=B.job_id AND B.event_function='EXECUTE_PQL' AND INSTR(B.event_limit, A.start_mode)>0")
                        .runPQL(TaskStatus.READY)
            }
        }

        dh.close()

        TaskRecorder.of(jobId, taskId, recordTime).log(s"Task $taskId of job $jobId at <$recordTime> status is ${if (status == TaskStatus.INITIALIZED) "not ready" else status} after pre-dependencies checking.")

        status == TaskStatus.READY
    }

    def checkTasksStatus(tick: String): Unit = {
        val dh = DataHub.QROSS

        //tasks to be restart
        dh.get(s"SELECT id AS task_id, job_id,  status FROM qross_tasks WHERE to_be_start_time='$tick'")
        if (dh.nonEmpty) {
            dh.cache("tasks")
            dh.openCache()
                .get(s"SELECT job_id, task_id FROM tasks WHERE status='${TaskStatus.CHECKING_LIMIT}'")
                    .put(s"UPDATE qross_tasks SET retry_times=0, status='${TaskStatus.INITIALIZED}' WHERE id=#task_id")
                .get(s"SELECT task_id FROM tasks WHERE status='${TaskStatus.INCORRECT}'")
                    .put("INSERT INTO qross_message_box (message_type, message_key, message_text) VALUES ('TASK', 'RESTART', '^WHOLE@#task_id')")
                .get(s"SELECT task_id FROM tasks WHERE status='${TaskStatus.FAILED}' OR status='${TaskStatus.TIMEOUT}'")
                    .put("INSERT INTO qross_message_box (message_type, message_key, message_text) VALUES ('TASK', 'RESTART', '^EXCEPTIONAL@#task_id')")
        }
        dh.clear()

        //stuck tasks - executing tasks but no running actions
        //get executing tasks
        dh.openQross()
            .get(
            s"""select id, job_id, task_id, command_id, upstream_ids, record_time, TIMESTAMPDIFF(SECOND, update_time, NOW()) AS span, status
                                FROM qross_tasks_dags WHERE task_id IN (SELECT id FROM qross_tasks WHERE status='${TaskStatus.EXECUTING}')""")
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
            dh.get("SELECT id, job_id, task_id FROM qross_stuck_records WHERE check_times>=3 AND renewed='no'")
                .put(s"UPDATE qross_tasks SET status='${TaskStatus.READY}', ready_time=NOW(), readiness=TIMESTAMP(SECOND, create_time, NOW()) WHERE id=#task_id AND status='${TaskStatus.EXECUTING}'")
                .put(s"UPDATE qross_stuck_records SET renewed='yes' where id=#id")
        }

        writeMessage("TaskStarter beat!")
        dh.set(s"UPDATE qross_keeper_beats SET last_beat_time=NOW() WHERE actor_name='TaskStarter'")

        dh.close()
    }

    //TaskStarter - execute()
    def getTaskCommandsToExecute(task: Task): DataTable = synchronized {

        val ds = DataSource.QROSS

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
                val map = ds.executeDataMap[String, Int](s"SELECT status, COUNT(0) AS amount FROM qross_tasks_dags WHERE task_id=$taskId AND record_time='$recordTime' GROUP BY status")
                if (map.isEmpty) {
                    //quit if no commands to execute
                    ds.executeNonQuery(s"UPDATE qross_tasks SET status='${TaskStatus.NO_COMMANDS}' WHERE id=$taskId")

                    TaskRecorder.of(jobId, taskId, recordTime).warn(s"Task $taskId of job $jobId at <$recordTime> has been  closed because no commands exists on task ready.")
                }
                else if (map.contains(ActionStatus.EXCEPTIONAL) || map.contains(ActionStatus.OVERTIME)) {
                    //restart task if exceptional or overtime
                    ds.executeNonQuery(s"INSERT INTO qross_message_box (message_type, message_key, message_text) VALUES ('TASK', 'RESTART', '^EXCEPTIONAL@$taskId')")
                    TaskRecorder.of(jobId, taskId, recordTime).warn(s"Task $taskId of job $jobId at <$recordTime> restart because EXCEPTIONAL commands exists on task ready.")
                }
                else if (map.contains(ActionStatus.WAITING) || map.contains(ActionStatus.QUEUEING) || map.contains(ActionStatus.RUNNING)) {
                    //executing
                    ds.executeNonQuery(s"UPDATE qross_tasks SET status='${TaskStatus.EXECUTING}', start_time=NOW(), latency=TIMESTAMPDIFF(SECOND, create_time, NOW()) WHERE id=$taskId")
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
            s"""SELECT A.action_id, A.job_id, A.task_id, A.command_id, B.task_time, B.record_time, B.start_mode, C.command_type, A.command_text, C.overtime, C.retry_limit, D.job_type, D.title, D.owner
                         FROM (SELECT id AS action_id, job_id, task_id, command_id, command_text FROM qross_tasks_dags WHERE task_id=$taskId AND record_time='$recordTime' AND status='${ActionStatus.WAITING}' AND upstream_ids='') A
                         INNER JOIN (SELECT id, task_time, record_time, start_mode FROM qross_tasks WHERE id=$taskId AND status='${TaskStatus.EXECUTING}') B ON A.task_id=B.id
                         INNER JOIN (SELECT id, command_type, overtime, retry_limit FROM qross_jobs_dags WHERE job_id=$jobId AND enabled='yes') C ON A.command_id=C.id
                         INNER JOIN (SELECT id, title, job_type, owner FROM qross_jobs WHERE id=$jobId) D ON A.job_id=D.id""")

        writeDebugging(s"Task $taskId of $jobId get ${executable.count()} commands to execute.")

        //prepare to run command - start time point
        ds.tableUpdate(s"UPDATE qross_tasks_dags SET start_time=NOW(), lagged=TIMESTAMPDIFF(SECOND, create_time, NOW()), status='${ActionStatus.QUEUEING}' WHERE id=#action_id", executable)

        ds.close()

        executable
    }

    //TaskExecutor
    def executeTaskCommand(taskCommand: DataRow): Task = {

        val dh = DataHub.QROSS

        val jobId = taskCommand.getInt("job_id")
        val jobType = taskCommand.getString("job_type")
        val taskId = taskCommand.getLong("task_id")
        val commandId = taskCommand.getInt("command_id")
        val actionId = taskCommand.getLong("action_id")
        val taskTime = taskCommand.getString("task_time")
        val retryLimit = taskCommand.getInt("retry_limit")
        val overtime = taskCommand.getInt("overtime")
        val recordTime = taskCommand.getString("record_time")
        val commandType = taskCommand.getString("command_type")

        //LET's GO!
        val logger = TaskRecorder.of(jobId, taskId, recordTime).run(commandId, actionId)

        var commandText = taskCommand.getString("command_text").replaceArguments(taskCommand)
        var error = false

        val PQL = new PQL("", DataHub.DEFAULT).set(taskCommand) //以变量方式传入

        if (commandType == "shell") {
            commandText = commandText.replace("%JAVA_BIN_HOME", Global.JAVA_BIN_HOME)
                                     .replace("%QROSS_HOME", Global.QROSS_HOME)
                                     .replace("%PQL", Global.PQL)

            //在Keeper中处理的好处是在命令的任何地方都可嵌入表达式
            try {
                commandText = commandText.$restore(PQL, "") //按PQL计算, 支持各种PQL嵌入式表达式, 但不保留引号
            }
            catch {
                case e: Exception =>
                    logger.err(e.getMessage)
                    error = true
            }

            //auto add environment variable to ahead
            commandText.takeBefore($BLANK).toLowerCase() match {
                case "java" => commandText = Global.JAVA_BIN_HOME + "java " + commandText.takeAfter($BLANK)
                case "pql" => commandText = Global.JAVA_BIN_HOME + s"java -jar ${Global.QROSS_HOME}qross-worker-${Global.QROSS_VERSION}.jar " + commandText.takeAfter($BLANK)
                case "python2" => commandText = Global.PYTHON2_HOME + "python2 " + commandText.takeAfter($BLANK)
                case "python3" => commandText = Global.PYTHON3_HOME + "python3 "+ commandText.takeAfter($BLANK)
                case _ =>
            }
        }

        dh.set(s"UPDATE qross_tasks_dags SET status='${if (error) ActionStatus.WRONG else ActionStatus.RUNNING}', command_text=?, run_time=NOW(), waiting=TIMESTAMPDIFF(SECOND, start_time, NOW()) WHERE id=$actionId", commandText)

        if (commandType == "pql") {
            commandText = Global.PQL + s" --task $actionId"
        }

        var exitValue = 1
        var continue = false
        var retry = -1

        if (error) {
            logger.err("Error occurred in command text or arguments, please check.")
        }
        else {
            var killed = false

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
                            //kill it
                            process.destroy()
                            killed = true
                            logger.warn(s"Action $actionId - command $commandId of task $taskId - job $jobId at <$recordTime> has been KILLED: $commandText")
                        }

                        Timer.sleep(1 seconds)
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

            logger.debug(s"FINISH action $actionId - command $commandId of task $taskId - job $jobId with exitValue $exitValue and status ${if (exitValue == 0) "SUCCESS" else if (exitValue > 0) "FAILURE" else "TIMEOUT/INTERRUPTED"}")

        }

        exitValue match {
            //finished
            case 0 =>
                //update DAG status
                dh.set(s"UPDATE qross_tasks_dags SET status='${ActionStatus.DONE}', elapsed=TIMESTAMPDIFF(SECOND, run_time, NOW()), finish_time=NOW(), retry_times=$retry WHERE id=$actionId")
                //update DAG dependencies
                dh.set(s"UPDATE qross_tasks_dags SET upstream_ids=REPLACE(upstream_ids, '($commandId)', '') WHERE task_id=$taskId AND record_time='$recordTime' AND status='${ActionStatus.WAITING}' AND POSITION('($commandId)' IN upstream_ids)>0")

                //if continue
                continue = dh.executeExists(s"SELECT id FROM qross_tasks_dags WHERE task_id=$taskId AND record_time='$recordTime' AND status='${ActionStatus.WAITING}' LIMIT 1")
                if (!continue) {
                    //meet: no waiting action, no running action
                    //action status: all done - task status: executing -> finished
                    //if exceptional action exists - task status: executing, finished -> failed

                    //update task status if all finished
                    dh.set(s"UPDATE qross_tasks SET finish_time=NOW(), spent=TIMESTAMPDIFF(SECOND, start_time, NOW()), status='${TaskStatus.FINISHED}', checked='' WHERE id=$taskId AND NOT EXISTS (SELECT id FROM qross_tasks_dags WHERE task_id=$taskId AND record_time='$recordTime' AND status!='${ActionStatus.DONE}')")

                    //check "after" dependencies
                    dh.get(s"SELECT A.id, A.task_id, A.record_time, A.dependency_type, A.dependency_label, A.dependency_content, A.dependency_option, A.ready, B.task_time FROM qross_tasks_dependencies A INNER JOIN qross_tasks B ON A.job_id=B.job_id WHERE B.status='${TaskStatus.FINISHED}' AND A.task_id=$taskId AND A.record_time='$recordTime' AND A.dependency_moment='after' AND A.ready='no'")
                            .foreach(row => {
                                if (TaskDependency.check(row) == "yes") {
                                    row.set("ready", "yes")
                                }
                            }).put("UPDATE qross_tasks_dependencies SET ready='#ready' WHERE id=#id")

                    dh.get(s"SELECT COUNT(0) AS amount FROM qross_tasks_dependencies WHERE task_id=$taskId AND record_time='$recordTime' AND dependency_moment='after' AND ready='no'")
                        .insertRowIfEmpty("amount" -> 0)
                        .put(s"UPDATE qross_tasks SET status=IF(#amount > 0, '${TaskStatus.INCORRECT}', '${TaskStatus.SUCCESS}'), checked=IF(#amount > 0, 'no', '') WHERE id=$taskId AND status='${TaskStatus.FINISHED}'")
                }
            //timeout
            case -1 =>
                dh.set(s"UPDATE qross_tasks_dags SET status='${ActionStatus.OVERTIME}', finish_time=NOW(), elapsed=TIMESTAMPDIFF(SECOND, run_time, NOW()), retry_times=$retry WHERE id=$actionId")
                dh.set(s"UPDATE qross_tasks SET status='${TaskStatus.TIMEOUT}', checked='no' WHERE id=$taskId")
            //killed
            case -2 =>
                dh.set(s"UPDATE qross_tasks_dags SET status='${ActionStatus.KILLED}', finish_time=NOW(), elapsed=TIMESTAMPDIFF(SECOND, run_time, NOW()) WHERE id=$actionId")
                dh.set(s"UPDATE qross_tasks SET status='${TaskStatus.INTERRUPTED}', killer=${TO_BE_KILLED(actionId)} WHERE id=$taskId")
                TO_BE_KILLED -= actionId
            //failed
            case _ =>
                dh.set(s"UPDATE qross_tasks_dags SET status='${ActionStatus.EXCEPTIONAL}', finish_time=NOW(), elapsed=TIMESTAMPDIFF(SECOND, run_time, NOW()), retry_times=$retry WHERE id=$actionId")
                dh.set(s"UPDATE qross_tasks SET finish_time=NOW(), status='${TaskStatus.FAILED}', checked='no' WHERE id=$taskId")
        }

        val status = dh.executeSingleValue(s"SELECT status FROM qross_tasks WHERE id=$taskId").asText("miss")
        //execute events
        if (status == TaskStatus.SUCCESS || status == TaskStatus.FAILED || status == TaskStatus.TIMEOUT || status == TaskStatus.INCORRECT) {

            dh.get(s"""SELECT A.job_id, A.event_name, A.event_function, A.event_limit, A.event_value, A.event_option, IFNULL(B.current_option, 0) AS current_option FROM
                    (SELECT job_id, event_name, event_function, event_limit, event_value, event_option FROM qross_jobs_events WHERE job_id=$jobId AND enabled='yes' AND event_name='onTask${status.capitalize}') A
                        LEFT JOIN
                    (SELECT job_id, event_function, COUNT(0) AS current_option FROM qross_tasks_events WHERE task_id=$taskId AND event_name='onTask${status.capitalize}' GROUP BY job_id, event_function) B
                    ON A.job_id=B.job_id AND A.event_function=B.event_function""")

            if (dh.nonEmpty) {
                dh.cache("events")
                dh.cache("task_info", new DataTable(taskCommand))

                dh.openCache()
                    .get("SELECT A.*, B.event_value AS receivers, B.event_limit, '' AS event_result, B.event_name, B.event_function FROM task_info A INNER JOIN events B ON A.job_id=B.job_id AND event_function='SEND_MAIL_TO' AND INSTR(B.event_limit, A.start_mode)>0")
                        .sendEmail(status)
                    .get("SELECT A.*, B.event_value AS api, B.event_option AS method, B.event_limit, '' AS event_result, B.event_name, B.event_function FROM task_info A INNER JOIN events B ON A.job_id=B.job_id AND B.event_function='REQUEST_API' AND INSTR(B.event_limit, A.start_mode)>0")
                        .requestApi(status)
                    .get(s"SELECT A.*, B.event_value AS delay, B.event_limit, '' AS to_be_start_time, '' AS event_result, B.event_name, B.event_function FROM task_info A INNER JOIN events B ON A.job_id=B.job_id WHERE B.event_function='RESTART_TASK' AND (B.event_option=0 OR B.current_option<B.event_option) AND INSTR(B.event_limit, A.start_mode)>0") //SIGNED is MySQL syntax, but SQLite will ignore it.
                        .restartTask(status)
                    .get(s"SELECT A.*, B.event_value AS roles, B.event_function, B.event_limit, '' AS event_result, B.event_name FROM task_info A INNER JOIN events B ON A.job_id=B.job_id AND INSTR(B.event_function, 'CUSTOM_')>0 AND INSTR(B.event_limit, A.start_mode)>0")
                        .fireCustomEvent(status)
                    .get("SELECT A.*, B.event_value AS pql, B.event_limit, '' AS event_result, B.event_name, B.event_function FROM task_info A INNER JOIN events B ON A.job_id=B.job_id AND B.event_function='EXECUTE_PQL' AND INSTR(B.event_limit, A.start_mode)>0")
                        .runPQL(status)
            }

            if (status != TaskStatus.SUCCESS) {
                dh.openQross()
                    .set(s"UPDATE qross_jobs SET unchecked_exceptional_tasks=IFNULL((SELECT GROUP_CONCAT(unchecked_exceptional_status ORDER BY unchecked_exceptional_status) AS exceptional_status FROM (SELECT CONCAT(`status`, ':', COUNT(0)) AS unchecked_exceptional_status FROM qross_tasks WHERE job_id=$jobId AND checked='no' GROUP BY `status`) T), '') WHERE id=$jobId;")
            }
            else {
                dh.openQross()
                    .set(s"UPDATE qross_tasks SET checked='yes' WHERE id=$taskId AND checked='no'")
            }
        }

        dh.close()

        if (continue) {
            //return
            Task(taskId, TaskStatus.EXECUTING).of(jobId).at(taskTime, recordTime)
        } else {
            //record and clear logger
            TaskRecorder.of(jobId, taskId, recordTime).debug(s"Task $taskId of job $jobId at <$recordTime> has finished with status ${status.toUpperCase}.").dispose()

            if (jobType != JobType.ENDLESS) {
                //return nothing
                Task(0)
            }
            else {
                //continue execute next task if job type is endless
                Task(-1, status).of(jobId)
            }
        }
    }
}