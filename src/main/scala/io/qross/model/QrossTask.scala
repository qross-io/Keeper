package io.qross.model

import java.io.File

import akka.actor.ActorSelection
import io.qross.core._
import io.qross.ext.Output._
import io.qross.ext.TypeExt._
import io.qross.fs.TextFile._
import io.qross.fs.{FileWriter, ResourceFile}
import io.qross.jdbc.DataSource
import io.qross.keeper.Keeper
import io.qross.net.{Http, Json}
import io.qross.pql.Patterns.$BLANK
import io.qross.pql.Solver._
import io.qross.pql.{PQL, Sharp}
import io.qross.script.Shell._
import io.qross.script.{Script, Shell}
import io.qross.setting.Global
import io.qross.time.TimeSpan._
import io.qross.time.{ChronExp, DateTime, Timer}
import io.qross.model.Qross.HashMap$Nodes

import scala.collection.mutable
import scala.sys.process._
import scala.util.control.Breaks.{break, breakable}

object QrossTask {

    val TO_BE_KILLED: mutable.HashMap[Long, Int] = new mutable.HashMap[Long, Int]()
    val EXECUTING: mutable.HashSet[Long] = new mutable.HashSet[Long]()

    implicit class DataHub$Task(dh: DataHub) {

        def sendEmail(taskStatus: String): DataHub = {
            if (Global.EMAIL_NOTIFICATION) {
                val upperStatus = taskStatus.toUpperCase()
                dh.openQross().foreach(row => {
                    val jobId = row.getInt("job_id")
                    val receivers = row.getString("receivers")
                    if (receivers != "") {
                        try {
                            row.set("event_result",
                                ResourceFile.open(s"/templates/$taskStatus.html")
                                    .replace("#{status}", upperStatus)
                                    .replaceWith(row)
                                    //.replace("#{logs}", if (dh.containsBuffer("logs")) TaskRecorder.toHTML(dh.takeOut("logs")) else "")
                                    .writeEmail(s"$upperStatus: ${row.getString("title")} ${row.getString("task_time")} - JobID: $jobId - TaskID: ${row.getString("task_id")}")
                                    //SELECT GROUP_CONCAT(CONCAT(B.fullname, '<', B.email, '>') SEPARATOR '; ') AS owner FROM qross_jobs_owners A INNER JOIN qross_users B ON A.user_id = B.id AND B.enabled = 'yes' AND A.job_id=" + row.getString("job_id")
                                    .to(if (receivers.contains("_OWNER")) row.getString("owner") else "")
                                    .to(if (receivers.contains("_LEADER")) {
                                        dh.executeSingleValue(s"SELECT GROUP_CONCAT(CONCAT(fullname, '<', email, '>') SEPARATOR '; ') AS leader FROM qross_users WHERE id IN (SELECT leader_id FROM qross_teams_members WHERE user_id IN (SELECT user_id FROM qross_jobs_owners WHERE job_id=$jobId)) AND enabled='yes'").asText("")
                                    } else "")
                                    .cc(if (receivers.contains("_KEEPER")) {
                                        dh.executeSingleValue("SELECT GROUP_CONCAT(CONCAT(fullname, '<', email, '>')) AS keeper FROM qross_users WHERE role='keeper' AND enabled='yes'").asText("")
                                    } else "")
                                    .bcc(if (receivers.contains("_MASTER")) {
                                        dh.executeSingleValue("SELECT GROUP_CONCAT(CONCAT(fullname, '<', email, '>')) AS master FROM qross_users WHERE role='master' AND enabled='yes'").asText("")
                                    } else "")
                                    .send())

                            TaskRecorder.of(row.getInt("job_id"), row.getLong("task_id"), row.getString("record_time"))
                                .debug(s"Task ${row.getLong("task_id")} of job ${row.getInt("job_id")} at <${row.getString("record_time")}> sent a mail on task $taskStatus")
                        }
                        catch {
                            case e: Exception =>
                                //由于模板问题造成的异常消息可能为null
                                var message = e.getMessage
                                if (message == null) {
                                    message = e.getCause.getMessage
                                    if (message == null) {
                                        message = "There may be errors in mail template."
                                    }
                                }

                                e.printStackTrace() //e.printReferMessage()
                                row.set("event_result", message)

                                TaskRecorder.of(row.getInt("job_id"), row.getLong("task_id"), row.getString("record_time"))
                                    .err(s"Task ${row.getLong("task_id")} of job ${row.getInt("job_id")} at <${row.getString("record_time")}> sent mail exception on task $taskStatus: " + message)
                        }
                    }
                    else {
                        row.set("event_result", "NO RECEIVERS")
                    }
                }).put("INSERT INTO qross_tasks_events (job_id, task_id, record_time, event_name, event_function, event_limit, event_value, event_result) VALUES (#job_id, #task_id, '#record_time', '#event_name', '#event_function', '#event_limit', '#receivers', '#event_result')")
                    .clear()
                    .openCache()
            }

            dh
        }

        def requestApi(taskStatus: String): DataHub = {
            dh.foreach(row => {
                val api = PQL.openEmbedded(row.getString("api")).asCommandOf(row.getInt("job_id")).place(row).run().toString.replace(" ", "%20").replace("&amp;", "&")
                val method = row.getString("method", "GET")

                if (api != "") {
                    val result = {
                        try {
                            Json.fromURL(api, method).toString()
                        }
                        catch {
                            case e: Exception => e.getMessage
                            case _: Throwable => ""
                        }
                    }

                    TaskRecorder.of(row.getInt("job_id"), row.getLong("task_id"), row.getString("record_time"))
                            .debug(s"Task ${row.getLong("task_id")} of job ${row.getInt("job_id")} at <${row.getString("record_time")}> requested api on task $taskStatus, result is $result")

                    row.set("event_result", result)
                }
                else {
                    row.set("event_result", "EMPTY API")
                }
                row.set("api", api)
            }).put("INSERT INTO qross_tasks_events (job_id, task_id, record_time, event_name, event_function, event_limit, event_value, event_result) VALUES (#job_id, #task_id, '#record_time', '#event_name', '#event_function', '#event_limit', '#api', '#event_result')")
                .clear()
        }

        def fireCustomEvent(taskStatus: String): DataHub = {
            dh.openQross().foreach(row => {
                val jobId = row.getInt("job_id")
                val value = row.getString("value") //value为传参
                val custom = row.getString("event_function").takeAfter("_")
                val script = dh.executeDataRow("SELECT event_value_type, event_script_type, event_logic FROM qross_keeper_custom_events WHERE id=" + custom)
                val scriptType = script.getString("event_script_type").toLowerCase()
                if (script.getString("event_value_type") == "roles") {
                        val owner = if (value.contains("_OWNER")) { dh.executeDataTable(s"SELECT id, username, fullname, email, CONCAT(fullname, '<', email, '>') AS personal, mobile, wechat_work_id FROM qross_users WHERE id IN (SELECT user_id FROM qross_jobs_owners WHERE job_id=$jobId) AND enabled='yes'") } else { new DataTable() }
                        val leader = if (value.contains("_LEADER")) { dh.executeDataTable(s"SELECT id, username, fullname, email, CONCAT(fullname, '<', email, '>') AS personal, mobile, wechat_work_id FROM qross_users WHERE id IN (SELECT leader_id FROM qross_teams_members WHERE user_id IN (SELECT user_id FROM qross_jobs_owners WHERE job_id=$jobId)) AND enabled='yes'") } else { new DataTable() }
                        val keeper = if (value.contains("_KEEPER")) { dh.executeDataTable("SELECT id, username, fullname, email, CONCAT(fullname, '<', email, '>') AS personal, mobile, wechat_work_id FROM qross_users WHERE role='keeper' AND enabled='yes'") } else { new DataTable() }
                        val master = if (value.contains("_MASTER")) { dh.executeDataTable("SELECT id, username, fullname, email, CONCAT(fullname, '<', email, '>') AS personal, mobile, wechat_work_id FROM qross_users WHERE role='master' AND enabled='yes'") } else { new DataTable() }
                        row.set("owner", owner, DataType.TABLE)
                        row.set("leader", leader, DataType.TABLE)
                        row.set("keeper", keeper, DataType.TABLE)
                        row.set("master", master, DataType.TABLE)
                        row.set("users", new DataTable().union(owner).union(leader).union(keeper).union(master), DataType.TABLE)
                }
                val logic = script.getString("event_logic")
                //execute
                try {
                    val result = {
                        scriptType match {
                            case "pql" => PQL.open(logic).asCommandOf(row.getInt("job_id")).place(row).run()
                            //shell和python支持PQL的嵌入表达式
                            case "shell" => Script.runShell(PQL.openEmbedded(logic).asCommandOf(row.getInt("job_id")).place(row).run().asInstanceOf[String])
                            case "python" => Script.runPython(PQL.openEmbedded(logic).asCommandOf(row.getInt("job_id")).place(row).run().asInstanceOf[String])

                        }
                    }
                    row.set("event_result", if (result == null) "SUCCESS" else result.toString)
                }
                catch {
                    case e: Exception =>
                        row.set("event_result", e.getMessage)
                        e.printStackTrace() //e.printReferMessage()
                }

                TaskRecorder.of(row.getInt("job_id"), row.getLong("task_id"), row.getString("record_time"))
                    .debug(s"Task ${row.getLong("task_id")} of job ${row.getInt("job_id")} at <${row.getString("record_time")}> fire custom event $custom on task $taskStatus.")

            }).put("INSERT INTO qross_tasks_events (job_id, task_id, record_time, event_name, event_function, event_limit, event_value, event_result) VALUES (#job_id, #task_id, '#record_time', '#event_name', '#event_function', '#event_limit', '#value', '#event_result')")
                .clear()
                .openCache()
        }

        def runScript(taskStatus: String): DataHub = {
            dh.foreach(row => {
                val script = row.getString("script")
                val scriptType = row.getString("script_type")

                try {
                    val result = {
                        scriptType.toLowerCase() match {
                            case "pql" => new PQL(script, DataHub.DEFAULT).asCommandOf(row.getInt("job_id")).place(row).run()
                            //shell和python支持PQL的嵌入表达式
                            case "shell" => Script.runShell(PQL.openEmbedded(script).asCommandOf(row.getInt("job_id")).place(row).run().asInstanceOf[String])
                            case "python" => Script.runPython(PQL.openEmbedded(script).asCommandOf(row.getInt("job_id")).place(row).run().asInstanceOf[String])
                            case _ => ""
                        }
                    }
                    row.set("event_result", if (result == null) "SUCCESS" else result.toString)
                }
                catch {
                    case e: Exception =>
                        row.set("event_result", e.getMessage)
                        e.printStackTrace() //e.printReferMessage()
                }

                TaskRecorder.of(row.getInt("job_id"), row.getLong("task_id"), row.getString("record_time"))
                        .debug(s"Task ${row.getLong("task_id")} of job ${row.getInt("job_id")} <${row.getString("record_time")}> run ${script.toUpperCase()} on task $taskStatus.")
            }).put("INSERT INTO qross_tasks_events (job_id, task_id, record_time, event_name, event_function, event_limit, event_value, event_option, event_result) VALUES (#job_id, #task_id, '#record_time', '#event_name', '#event_function', '#event_limit', '#script', '#script_type', '#event_result')")
                .clear()
        }

        def restartTask(taskStatus: String): DataHub = {
            if (dh.nonEmpty) {
                dh.put("INSERT INTO qross_tasks_events (job_id, task_id, record_time, event_name, event_function, event_limit, event_value, event_result) VALUES (#job_id, #task_id, '#record_time', '#event_name', '#event_function', '#event_limit', '#delay', 'RESTARTED.')")
                .foreach(row => {
                    val delay = row.getInt("delay", 30)
                    row.set("to_be_start_time", DateTime.now.plusMinutes(delay).getTickValue)

                    TaskRecorder.of(row.getInt("job_id"), row.getLong("task_id"), row.getString("record_time")).debug(s"Task ${row.getLong("task_id")} of job ${row.getInt("job_id")} <${row.getString("record_time")}> will restart after $delay minutes by $taskStatus.")
                })
                .put(s"INSERT INTO qross_tasks_to_be_start (job_id, task_id, task_time, record_time, status, to_be_start_time) VALUES (#job_id, #task_id, '#task_time', '#record_time', '$taskStatus', '#to_be_start_time')")
            }

            dh.clear()
        }

        def generateDependencies(): DataHub = {

            if (dh.nonEmpty) {
                dh.foreach(row => {
                    row.getString("dependency_type").toUpperCase() match {
                        case "TASK" =>
                            val sharp = row.getString("dependency_content")
                            row.set("dependency_content",
                                if (sharp != null) {
                                    """(?i)^\$task_time\s""".r.findFirstIn(sharp) match {
                                        case Some(time) =>
                                            try {
                                                new Sharp(sharp.takeAfter(time).trim(), DataCell(row.getDateTime("task_time"), DataType.DATETIME)).execute().asText
                                            }
                                            catch {
                                                case e: Exception =>
                                                    e.printStackTrace() //e.printReferMessage()
                                                    TaskRecorder.of(row.getInt("job_id"), row.getLong("task_id"), row.getString("record_time"))
                                                        .warn(s"Wrong task time expression: " + sharp)
                                                        .err(e.getMessage)
                                                    "WRONG"
                                            }
                                        case None =>
                                            if ("""(?i)^[a-z]+\s""".r.test(sharp)) {
                                                new Sharp(sharp, DataCell(row.getDateTime("task_time"), DataType.DATETIME)).execute().asText
                                            }
                                            else {
                                                row.getDateTime("task_time").format(sharp)
                                            }
                                    }
                                }
                                else {
                                    TaskRecorder.of(row.getInt("job_id"), row.getLong("task_id"), row.getString("record_time"))
                                        .warn(s"Wrong task time expression: " + sharp)
                                        .err("Empty Task time expression.")
                                    ""
                                }, DataType.TEXT)
                        case "SQL" =>
                            val PQL = new PQL("", DataHub.DEFAULT).asCommandOf(row.getInt("job_id")).place(row)
                            try {
                                row.set("dependency_content",
                                    row.getString("dependency_content")
                                        .replaceArguments(row)
                                        .$restore(PQL), DataType.TEXT)
                                row.set("dependency_option",
                                    row.getString("dependency_option")
                                        .replaceArguments(row)
                                        .$restore(PQL), DataType.TEXT)
                            }
                            catch {
                                case e: Exception =>
                                    TaskRecorder.of(row.getInt("job_id"), row.getLong("task_id"), row.getString("record_time"))
                                        .err("Wrong dependency configuration. Please check and correct it.")
                                        .err(e.getMessage)

                            }
                        case "PQL" =>
                            row.set("dependency_content", row.getString("dependency_content").replaceArguments(row), DataType.TEXT)
                            row.set("dependency_option", row.getString("dependency_option").replaceArguments(row), DataType.TEXT)
                    }
                }).put("INSERT INTO qross_tasks_dependencies (job_id, task_id, record_time, dependency_id, dependency_moment, dependency_type, dependency_label, dependency_content, dependency_option) VALUES (#job_id, #task_id, '#record_time', #dependency_id, '#dependency_moment', '#dependency_type', '#dependency_label', '#dependency_content', '#dependency_option')")
            }

            dh.clear()
        }
    }

    //on keeper start up @ TaskProducer
    def complementTasks(): mutable.ArrayBuffer[Task] = {

        /*
        TimeLine:
        last beat -> server offline -> now -> server online -> next beat
        */

        val dh = DataHub.QROSS
        val address = Keeper.NODE_ADDRESS
        //Only takes effect when the first node starts
        if (dh.executeSingleValue("SELECT COUNT(0) AS nodes FROM qross_keeper_nodes WHERE status='online'").asInteger(0) == 1) {
            //get last tick of producer
            val lastBeat = dh.executeSingleValue("SELECT MIN(last_beat_time) AS last_beat_time FROM qross_keeper_beats WHERE actor_name='TaskProducer'")
                .asDateTimeOrElse(DateTime.now)
                .setSecond(0)
                .setNano(0)
                .plusMinutes(1)
            val nextBeat = DateTime.now.setSecond(0).setNano(0).plusMinutes(1)

            //get all jobs
            dh.openQross()
                //next_tick != '' means this is not a new job
                .get(s"SELECT id AS job_id, cron_exp, next_tick, complement_missed_tasks FROM qross_jobs WHERE job_type='${JobType.SCHEDULED}' AND enabled='yes' AND next_tick<>'' AND next_tick<>'NONE' AND next_tick<'${nextBeat.getTickValue}'")
                .cache("jobs")

            //get all ticks for jobs that need to complement during server was offline
            dh.openCache()
                .get("SELECT job_id, cron_exp, next_tick FROM jobs WHERE complement_missed_tasks='yes'")
                .table("job_id" -> DataType.INTEGER, "next_tick" -> DataType.TEXT)(row => {
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
                .get(s"SELECT job_id, task_time FROM qross_tasks WHERE job_id IN (SELECT id FROM qross_jobs WHERE job_type='${JobType.SCHEDULED}' AND enabled='yes' AND next_tick<>'' AND next_tick<>'NONE' AND complement_missed_tasks='yes') AND task_time>'${lastBeat.getTickValue}' AND task_time<'${nextBeat.getTickValue}'")
                .cache("exists_tasks")

            val recordTime = DateTime.now.getString("yyyy-MM-dd HH:mm:ss")
            //complement all jobs
            dh.openCache()
                .get(s"SELECT A.job_id, A.next_tick FROM missed_tasks A LEFT JOIN exists_tasks B ON A.job_id=B.job_id AND A.next_tick=B.task_time WHERE B.job_id IS NULL")
                    .put(s"INSERT IGNORE INTO qross_tasks (job_id, task_time, record_time, create_mode, start_mode) VALUES (?, ?, '$recordTime', 'complement', 'auto_start')")
                .openQross()
                .pass(s"SELECT job_id, id AS task_id, task_time, record_time FROM qross_tasks WHERE job_id=#job_id AND task_time='#next_tick'", "job_id" -> 0, "next_tick" -> "")
                    .put(s"INSERT IGNORE INTO qross_tasks_living (job_id, task_id, task_time, record_time) VALUES (?, ?, ?, ?)")
                    .set(s"INSERT INTO qross_tasks_stats (node_address, moment, created_tasks) VALUES ('$address', '${recordTime.dropRight(5) + "00:00"}', ${dh.AFFECTED_ROWS_OF_LAST_PUT}) ON DUPLICATE KEY UPDATE created_tasks=created_tasks+${dh.AFFECTED_ROWS_OF_LAST_PUT}")
                    .clear()

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
        }

        //to be restart
        val tasks = new mutable.ArrayBuffer[Task]()

        //restart executing tasks when Keeper exit exceptionally.
        dh.openQross()
             .get(s"SELECT task_id, record_time FROM qross_tasks_living WHERE status='${TaskStatus.EXECUTING}' AND node_address='$address'")
                .put(s"UPDATE qross_tasks_dags SET status='${ActionStatus.EXCEPTIONAL}' WHERE task_id=#task_id AND record_time='#record_time' AND status IN ('${ActionStatus.QUEUEING}', '${ActionStatus.RUNNING}')")
                .foreach(row => {
                    tasks += restartTask(row.getLong("task_id"), "^EXCEPTIONAL")
                })

        dh.close()

        tasks
    }

    //TaskProducer
    //create and initialize tasks then return initialized and ready tasks
    def createAndInitializeTasks(tick: String): mutable.ArrayBuffer[Task] = {
        val minute = new DateTime(tick)
        val address = Keeper.NODE_ADDRESS

        val dh = DataHub.QROSS

        val locked: Boolean = dh.executeNonQuery(s"UPDATE qross_keeper_locks SET node_address='$address', tick='$tick', lock_time=NOW() WHERE lock_name='CREATE-TASKS' AND tick<>'$tick'") == 1
        if (locked) {
            //update empty next_tick - it will be empty when create a new job
            //update outdated jobs - it will occur when you enable a job from disabled
            dh
                //next_tick will be NONE if cron exp is expired.
                .get(s"SELECT id AS job_id, cron_exp, '' AS next_tick, enabled FROM qross_jobs WHERE job_type='${JobType.SCHEDULED}' AND enabled='yes' AND (next_tick='' OR (next_tick<>'NONE' AND next_tick<'$tick'))")
                .foreach(row =>
                    try {
                        row.set("next_tick", ChronExp(row.getString("cron_exp")).getNextTickOrNone(minute))
                    }
                    catch {
                        case e: Exception =>
                            e.printStackTrace() //e.printReferMessage()
                            row.set("next_tick", "INCORRECT-CRON-EXPRESSION")
                            row.set("enabled", "no")
                    }
                ).put("UPDATE qross_jobs SET next_tick='#next_tick', enabled='#enabled' WHERE id=#job_id")

            //create tasks without cron_exp
            //excluding jobs with executing tasks
            val now = DateTime.now.getString("yyyy-MM-dd HH:mm:ss")
            dh.get(
                s"""SELECT id AS job_id FROM qross_jobs WHERE job_type='${JobType.DEPENDENT}' AND enabled='yes' AND id NOT IN (SELECT DISTINCT job_id FROM qross_tasks_living)""")
                .put(s"INSERT IGNORE INTO qross_tasks (job_id, task_time, record_time, create_mode, start_mode) VALUES (#job_id, '$now', '$now', 'trigger', 'auto_start')")
            .pass(s"SELECT job_id, id AS task_id, task_time, record_time FROM qross_tasks WHERE job_id=#job_id AND task_time='$now'", "job_id" -> 0)
                .put("INSERT IGNORE INTO qross_tasks_living (job_id, task_id, task_time, record_time) VALUES (?, ?, ?, ?)")
                .set(s"INSERT INTO qross_tasks_stats (node_address, moment, created_tasks) VALUES ('$address', '${now.dropRight(5) + "00:00"}', ${dh.AFFECTED_ROWS_OF_LAST_PUT}) ON DUPLICATE KEY UPDATE created_tasks=created_tasks+${dh.AFFECTED_ROWS_OF_LAST_PUT}")
                    .clear()

            //jobs with cron_exp - scheduled
            dh.get(s"SELECT id AS job_id, cron_exp, next_tick FROM qross_jobs WHERE next_tick='$tick' AND job_type='${JobType.SCHEDULED}' AND enabled='yes'")
                //create schedule tasks
                .put(s"INSERT IGNORE INTO qross_tasks (job_id, task_time, record_time, create_mode, start_mode) VALUES (#job_id, '#next_tick', '${DateTime.now.toString}', 'schedule', 'auto_start')")
                //get next tick and update
                .foreach(row => {
                    try {
                        row.set("next_tick", ChronExp(row.getString("cron_exp")).getNextTickOrNone(minute.plusMinutes(1))) //get next minute to match next tick
                    }
                    catch {
                        case e: Exception => writeException(e.getMessage)
                    }
                })
                .put("UPDATE qross_jobs SET next_tick='#next_tick' WHERE id=#job_id")
            .pass(s"SELECT job_id, id AS task_id, task_time, record_time FROM qross_tasks WHERE job_id=#job_id AND task_time='$tick'", "job_id" -> 0)
                .put("INSERT IGNORE INTO qross_tasks_living (job_id, task_id, task_time, record_time) VALUES (?, ?, ?, ?)")
                .set(s"INSERT INTO qross_tasks_stats (node_address, moment, created_tasks) VALUES ('$address', '${now.dropRight(5) + "00:00"}', ${dh.AFFECTED_ROWS_OF_LAST_PUT}) ON DUPLICATE KEY UPDATE created_tasks=created_tasks+${dh.AFFECTED_ROWS_OF_LAST_PUT}")
                    .clear()

            //get all new tasks
            dh.get(
                s"""SELECT A.task_id, A.job_id, C.title, C.owner, A.task_time, A.record_time, A.start_mode, IFNULL(B.dependencies, 0) AS dependencies
                   FROM (SELECT task_id, job_id, task_time, record_time, start_mode FROM qross_tasks_living WHERE status='${TaskStatus.NEW}') A
                   INNER JOIN qross_jobs C ON A.job_id=C.id
                   LEFT JOIN (SELECT job_id, COUNT(0) AS dependencies FROM qross_jobs_dependencies WHERE dependency_moment='before' AND enabled='yes' GROUP BY job_id) B ON A.job_id=B.job_id""")

            if (dh.nonEmpty) {
                //onTaskNew events
                dh.cache("tasks")
                    .openCache()
                        //update status
                        .get("SELECT GROUP_CONCAT(job_id) AS job_ids FROM tasks")
                    .openQross()
                        .pass("SELECT job_id, event_name, event_function, event_limit, event_value, event_option FROM qross_jobs_events FORCE INDEX (idx_qross_jobs_events_select) WHERE job_id IN (#job_ids) AND enabled='yes' AND event_name='onTaskNew'", "job_ids" -> 0)

                if (dh.nonEmpty) {
                    dh.cache("events")
                    //onTaskNew Event
                    dh.openCache()
                        .get("SELECT A.task_id, A.job_id, A.title, A.owner, A.task_time, A.record_time, B.event_value AS receivers, B.event_name, B.event_function, B.event_limit, '' AS event_result FROM tasks A INNER JOIN events B ON A.job_id=B.job_id AND B.event_function='SEND_MAIL_TO' AND INSTR(B.event_limit, A.start_mode)>0")
                            .sendEmail(TaskStatus.NEW)
                        .get("SELECT A.task_id, A.job_id, A.title, A.owner, A.task_time, A.record_time, B.event_value AS api, B.event_option AS method, B.event_name, B.event_function, B.event_limit, '' AS event_result FROM tasks A INNER JOIN events B ON A.job_id=B.job_id AND B.event_function='REQUEST_API' AND INSTR(B.event_limit, A.start_mode)>0")
                            .requestApi(TaskStatus.NEW)
                        .get("SELECT A.task_id, A.job_id, A.title, A.owner, A.task_time, A.record_time, B.event_value AS value, B.event_name, B.event_function, B.event_limit, '' AS event_result FROM tasks A INNER JOIN events B ON A.job_id=B.job_id AND INSTR(B.event_function, 'CUSTOM_')>0 AND INSTR(B.event_limit, A.start_mode)>0")
                            .fireCustomEvent(TaskStatus.NEW)
                        .get("SELECT A.task_id, A.job_id, A.title, A.owner, A.task_time, A.record_time, B.event_option AS script_type, B.event_value AS script, B.event_name, B.event_function, B.event_limit, '' AS event_result FROM tasks A INNER JOIN events B ON A.job_id=B.job_id AND B.event_function='EXECUTE_SCRIPT' AND INSTR(B.event_limit, A.start_mode)>0")
                            .runScript(TaskStatus.NEW)
                }
                dh.clear()

                // ----- dependencies -----

                //get all dependencies
                dh.openCache()
                    .get("SELECT IFNULL(GROUP_CONCAT(DISTINCT job_id), 0) AS job_ids FROM tasks WHERE dependencies>0")
                dh.openQross()
                    .pass("SELECT id AS dependency_id, job_id, dependency_moment, dependency_type, dependency_label, dependency_content, dependency_option FROM qross_jobs_dependencies WHERE job_id IN (#job_ids) AND enabled='yes'")
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
                    .pass("SELECT A.id AS command_id, IF(B.id IS NULL, A.command_text, B.command_logic) AS command_text, A.args, A.job_id, A.upstream_ids FROM qross_jobs_dags A LEFT JOIN qross_commands_templates B ON A.template_id=B.id WHERE A.job_id IN (#job_ids) AND A.enabled='yes'")
                    .cache("dags")

                //generate DAGs
                dh.openCache()
                    .get("SELECT A.job_id, A.task_id, A.record_time, B.command_id, B.command_text, B.args, B.upstream_ids FROM tasks A INNER JOIN dags B ON A.job_id=B.job_id")
                    .put("INSERT INTO qross_tasks_dags (job_id, task_id, record_time, command_id, command_text, args, upstream_ids) VALUES (?, ?, ?, ?, ?, ?, ?)")

                //update tasks status
                dh.get(s"SELECT task_id FROM tasks WHERE dependencies>0")
                    .put(s"UPDATE qross_tasks SET status='${TaskStatus.INITIALIZED}' WHERE id=#task_id")
                    .put(s"UPDATE qross_tasks_living SET status='${TaskStatus.INITIALIZED}' WHERE task_id=#task_id")
                .get("SELECT task_id FROM tasks WHERE dependencies=0")
                    .put(s"UPDATE qross_tasks SET status='${TaskStatus.READY}', ready_time=NOW(), readiness=TIMESTAMPDIFF(SECOND, create_time, NOW()) WHERE id=#task_id")
                    .put(s"UPDATE qross_tasks_living SET status='${TaskStatus.READY}' WHERE task_id=#task_id")

                //Master will can't turn on job if no commands to execute - 2018.9.8
                dh.get("SELECT A.job_id, A.task_id FROM tasks A LEFT JOIN dags B ON A.job_id=B.job_id WHERE B.job_id IS NULL")
                    .put(s"UPDATE qross_tasks SET status='${TaskStatus.NO_COMMANDS}' WHERE id=#task_id")
                    .put("DELETE FROM qross_tasks_living WHERE task_id=#task_id")
            }
        }

        // ---------- finishing ----------

        dh.openQross()

        val prepared = new mutable.ArrayBuffer[Task]()

        if (locked) {
            //send initialized tasks to checker, and send ready tasks to starter
            dh.get(s"SELECT task_id, job_id, task_time, record_time, status FROM qross_tasks_living WHERE status='${TaskStatus.INITIALIZED}' OR status='${TaskStatus.READY}'")
                .foreach(row => {
                    prepared += Task(row.getLong("task_id"), row.getString("status")).of(row.getInt("job_id")).at(row.getString("task_time"), row.getString("record_time"))
                })
        }

        //beat
        dh.set(s"UPDATE qross_keeper_beats SET last_beat_time=NOW() WHERE node_address='$address' AND actor_name='TaskProducer'")
        writeLineWithSeal("SYSTEM", s"<$address> TaskProducer beat!")

        dh.close()

        prepared
    }

    def createEndlessTask(jobId: Int): Task = {

        val dh = DataHub.QROSS

        val recordTime = DateTime.now.getString("yyyy-MM-dd HH:mm:ss")
        val taskTime = recordTime

        dh.set(s"INSERT IGNORE INTO qross_tasks (job_id, task_time, record_time, create_mode, start_mode) VALUES ($jobId, '$taskTime', '$recordTime', 'interval', 'auto_start')")

        val taskId = dh.executeSingleValue(s"SELECT id FROM qross_tasks WHERE job_id=$jobId AND task_time='$taskTime'").asInteger

        dh.set(s"INSERT IGNORE INTO qross_tasks_living (job_id, task_id, task_time, record_time) VALUES ($jobId, $taskId, '$taskTime', '$recordTime')")
          .set(s"INSERT INTO qross_tasks_stats (node_address, moment, created_tasks) VALUES ('${Keeper.NODE_ADDRESS}', '${recordTime.dropRight(5) + "00:00"}', 1) ON DUPLICATE KEY UPDATE created_tasks=created_tasks+1")
          .get(
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
                .get("SELECT A.task_id, A.job_id, A.title, A.owner, A.task_time, A.record_time, B.event_value AS value, B.event_name, B.event_function, B.event_limit, '' AS event_result FROM tasks A INNER JOIN events B ON A.job_id=B.job_id AND INSTR(B.event_function, 'CUSTOM_')>0 AND INSTR(B.event_limit, A.start_mode)>0")
                    .fireCustomEvent(TaskStatus.NEW)
                .get("SELECT A.task_id, A.job_id, A.title, A.owner, A.task_time, A.record_time, B.event_option AS script_type, B.event_value AS script, B.event_name, B.event_function, B.event_limit, '' AS event_result FROM task_info A INNER JOIN events B ON A.job_id=B.job_id AND event_function='EXECUTE_SCRIPT' AND INSTR(B.event_limit, A.start_mode)>0")
                    .runScript(TaskStatus.NEW)
        }
        dh.clear()

        // ---------- DAGs ----------
        dh.openQross()
            .get(s"SELECT A.id AS command_id, IF(B.id IS NULL, A.command_text, B.command_logic) AS command_text, A.args, A.job_id, A.upstream_ids FROM qross_jobs_dags A LEFT JOIN qross_commands_templates B ON A.template_id=B.id WHERE A.job_id=$jobId AND A.enabled='yes'")
                .cache("dags")
        //generate DAGs
        dh.openCache()
            .get("SELECT A.job_id, A.task_id, A.record_time, B.command_id, B.command_text, B.args, B.upstream_ids FROM task_info A INNER JOIN dags B ON A.job_id=B.job_id")
                .put("INSERT INTO qross_tasks_dags (job_id, task_id, record_time, command_id, command_text, args, upstream_ids) VALUES (?, ?, ?, ?, ?, ?, ?)")

        //update tasks status
        dh.openQross()
            .set(s"UPDATE qross_tasks SET status='${TaskStatus.READY}', ready_time=NOW(), readiness=TIMESTAMPDIFF(SECOND, create_time, NOW()) WHERE id=$taskId")
            .set(s"UPDATE qross_tasks_living SET status='${TaskStatus.READY}' WHERE task_id=$taskId")

        dh.close()

        Task(taskId, TaskStatus.READY).of(jobId).at(taskTime, recordTime)
    }

    //选择 DAG 中的部分 Command 和修改 DAG 中的 Command 功能移到客户端
    def createInstantWholeTask(jobId: Int, delay: Int = 0, ignoreDepends: String = "no", creator: Int = 0): Task = {

        val dh = DataHub.QROSS

        val taskTime = DateTime.now.getString("yyyy-MM-dd HH:mm:ss")
        val recordTime = taskTime
        val status = if (ignoreDepends == "yes") TaskStatus.READY else TaskStatus.INITIALIZED

        //create task
        dh.set(s"INSERT IGNORE INTO qross_tasks (job_id, node_address, task_time, record_time, status, creator, create_mode, start_mode) VALUES ($jobId, '${Keeper.NODE_ADDRESS}', '$taskTime', '$recordTime', '${TaskStatus.INSTANT}', $creator, 'instant', 'manual_start')")
          .get(s"""SELECT A.task_id, A.job_id, A.task_time, A.record_time, A.start_mode, B.title, B.owner
                    FROM (SELECT id AS task_id, job_id, task_time, record_time, start_mode FROM qross_tasks WHERE job_id=$jobId AND task_time='$taskTime' AND status='${TaskStatus.INSTANT}') A
                    INNER JOIN qross_jobs B ON A.job_id=B.id""")
         .put(s"INSERT IGNORE INTO qross_tasks_living (job_id, task_id, task_time, record_time, start_mode) VALUES ($jobId, #task_id, '$taskTime', '$recordTime', 'manual_start')")
         .set(s"INSERT INTO qross_tasks_stats (node_address, moment, created_tasks) VALUES ('${Keeper.NODE_ADDRESS}', '${recordTime.dropRight(5) + "00:00"}', ${dh.AFFECTED_ROWS_OF_LAST_PUT}) ON DUPLICATE KEY UPDATE created_tasks=created_tasks+${dh.AFFECTED_ROWS_OF_LAST_PUT}")
            .cache("task_info")

        //get task id
        val taskId = dh.firstRow.getLong("task_id")

        //events
        dh.get(s"SELECT job_id, event_name, event_function, event_limit, event_value, event_option FROM qross_jobs_events WHERE job_id=$jobId AND enabled='yes' AND event_name='onTaskNew'")
        if (dh.nonEmpty) {
            dh.cache("events")
            //onTaskNew event
            dh.openCache()
                .get("SELECT A.task_id, A.job_id, A.title, A.owner, A.task_time, A.record_time, B.event_value AS receivers, B.event_limit, '' AS event_result, B.event_name, B.event_function FROM task_info A INNER JOIN events B ON A.job_id=B.job_id AND B.event_function='SEND_MAIL_TO' AND INSTR(B.event_limit, A.start_mode)>0")
                    .sendEmail(TaskStatus.NEW)
                .get("SELECT A.task_id, A.job_id, A.title, A.owner, A.task_time, A.record_time, B.event_value AS api, B.event_option AS method, B.event_limit, '' AS event_result, B.event_name, B.event_function FROM task_info A INNER JOIN events B ON A.job_id=B.job_id AND B.event_function='REQUEST_API' AND INSTR(B.event_limit, A.start_mode)>0")
                    .requestApi(TaskStatus.NEW)
                .get("SELECT A.task_id, A.job_id, A.title, A.owner, A.task_time, A.record_time, B.event_value AS value, B.event_name, B.event_function, B.event_limit, '' AS event_result FROM task_info A INNER JOIN events B ON A.job_id=B.job_id AND INSTR(B.event_function, 'CUSTOM_')>0 AND INSTR(B.event_limit, A.start_mode)>0")
                    .fireCustomEvent(TaskStatus.NEW)
                .get("SELECT A.task_id, A.job_id, A.title, A.owner, A.task_time, A.record_time, B.event_option AS script_type, B.event_value AS script, B.event_limit, '' AS event_result, B.event_name, B.event_function FROM task_info A INNER JOIN events B ON A.job_id=B.job_id AND B.event_function='EXECUTE_SCRIPT' AND INSTR(B.event_limit, A.start_mode)>0")
                    .runScript(TaskStatus.NEW)
        }

        //dependencies
        dh.openQross()
            .get(s"SELECT job_id, $taskId AS task_id, '$taskTime' AS task_time, '$recordTime' AS record_time, id AS dependency_id, dependency_moment, dependency_type, dependency_label, dependency_content, dependency_option FROM qross_jobs_dependencies WHERE job_id=$jobId AND enabled='yes'" + (if (ignoreDepends == "yes") " AND dependency_moment='after'" else ""))
                .generateDependencies()

        //DAG
        dh.get(s"SELECT A.job_id, $taskId AS task_id, A.id AS command_id, IF(B.id IS NULL, A.command_text, B.command_logic) AS command_text, A.args, A.upstream_ids FROM qross_jobs_dags A LEFT JOIN qross_commands_templates B ON A.template_id=B.id WHERE A.job_id=$jobId AND A.enabled='yes'")
            .put(s"INSERT INTO qross_tasks_dags (job_id, task_id, record_time, command_id, command_text, args, upstream_ids) VALUES (?, ?, '$recordTime', ?, ?, ?, ?)")

        //update task status
        if (status == TaskStatus.READY) {
            dh.set(s"UPDATE qross_tasks SET status='${TaskStatus.READY}', ready_time=NOW(), readiness=TIMESTAMPDIFF(SECOND, create_time, NOW()) WHERE id=$taskId")
              .set(s"UPDATE qross_tasks_living SET status='${TaskStatus.READY}' WHERE task_id=$taskId")
        }
        else {
            dh.set(s"UPDATE qross_tasks SET status='${TaskStatus.INITIALIZED}' WHERE id=$taskId")
              .set(s"UPDATE qross_tasks_living SET status='${TaskStatus.INITIALIZED}' WHERE task_id=$taskId")
        }

        dh.close()

        TaskRecorder.of(jobId, taskId, recordTime).debug(s"Instant Task $taskId of job $jobId <$recordTime> has been created.")

        //delay
        if (delay > 0) {
            TaskRecorder.of(jobId, taskId, recordTime).debug(s"Instant Task $taskId of job $jobId <$recordTime> will start after $delay seconds.")
            Timer.sleep({ if (delay > 60) 60 else delay }.seconds)
        }

        Task(taskId, status).of(jobId).at(taskTime, recordTime)
    }

    def createInstantTask(message: String, creator: Int): Task = {

        /*
            {
                jobId: 123,
                dag: "1,2,3",
                args: "commandId:args##$##commandId:args",
                commands: "commandId:commandText##$##commandId:commandText",
                delay: 5,  //s 延时
                ignoreDepends: 'yes' //是否忽略前置依赖
            }
         */

        val info = Json(message).parseRow("/")

        val jobId = info.getInt("jobId")
        var taskId = 0L
        val dag = info.getString("dag")
        val args = info.getString("args").decodeURL().splitToMap("##\\$##", ":") //to replace #{param} in command text
        val commands = info.getString("commands").decodeURL().splitToMap("##\\$##", ":")
        val delay = info.getInt("delay")
        val taskTime = DateTime.now.getString("yyyy-MM-dd HH:mm:ss")
        val recordTime = DateTime.now.toString()
        val ignore = info.getString("ignoreDepends", "no")

        //status
        val status = if (ignore == "yes") TaskStatus.READY else TaskStatus.INITIALIZED

        if (jobId > 0) {

            val dh = DataHub.QROSS

            //create task
            dh.set(s"INSERT IGNORE INTO qross_tasks (node_address, job_id, task_time, record_time, status, creator, create_mode, start_mode) VALUES ('${Keeper.NODE_ADDRESS}', $jobId, '$taskTime', '$recordTime', '${TaskStatus.INSTANT}', $creator, 'instant', 'manual_start')")
              .get(s"""SELECT A.task_id, A.job_id, A.task_time, A.record_time, A.start_mode, B.title, B.owner
                        FROM (SELECT id AS task_id, job_id, task_time, record_time, start_mode FROM qross_tasks WHERE job_id=$jobId AND task_time='$taskTime' AND status='${TaskStatus.INSTANT}') A
                        INNER JOIN qross_jobs B ON A.job_id=B.id""")
                .put(s"INSERT IGNORE INTO qross_tasks_living (job_id, task_id, task_time, record_time, start_mode) VALUES ($jobId, #task_id, '$taskTime', '$recordTime', 'manual_start')")
                .set(s"INSERT INTO qross_tasks_stats (node_address, moment, created_tasks) VALUES ('${Keeper.NODE_ADDRESS}', '${recordTime.dropRight(5) + "00:00"}', ${dh.AFFECTED_ROWS_OF_LAST_PUT}) ON DUPLICATE KEY UPDATE created_tasks=created_tasks+${dh.AFFECTED_ROWS_OF_LAST_PUT}")
                .cache("task_info")

            //get task id
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
                        .get("SELECT A.task_id, A.job_id, A.title, A.owner, A.task_time, A.record_time, B.event_value AS value, B.event_name, B.event_function, B.event_limit, '' AS event_result FROM task_info A INNER JOIN events B ON A.job_id=B.job_id AND INSTR(B.event_function, 'CUSTOM_')>0 AND INSTR(B.event_limit, A.start_mode)>0")
                            .fireCustomEvent(TaskStatus.NEW)
                        .get("SELECT A.task_id, A.job_id, A.title, A.owner, A.task_time, A.record_time, B.event_option AS script_type, B.event_value AS script, B.event_limit, '' AS event_result, B.event_name, B.event_function FROM task_info A INNER JOIN events B ON A.job_id=B.job_id AND B.event_function='EXECUTE_SCRIPT' AND INSTR(B.event_limit, A.start_mode)>0")
                            .runScript(TaskStatus.NEW)
                }

                //dependencies
                dh.openQross()
                    .get(s"SELECT job_id, $taskId AS task_id, '$taskTime' AS task_time, '$recordTime' AS record_time, id AS dependency_id, dependency_moment, dependency_type, dependency_label, dependency_content, dependency_option FROM qross_jobs_dependencies WHERE job_id=$jobId AND enabled='yes'" + (if (ignore == "yes") " AND dependency_moment='after'" else ""))
                        .generateDependencies()
                //DAG
                dh.get(s"SELECT A.job_id, $taskId AS task_id, A.id AS command_id, IF(B.id IS NULL, A.command_text, B.command_logic) AS command_text, A.args, A.upstream_ids FROM qross_jobs_dags A LEFT JOIN qross_commands_templates B ON A.template_id=B.id WHERE A.job_id=$jobId" + (if (dag != "") s" AND A.id IN ($dag)" else ""))
                //replace commands and args
                if (args.nonEmpty || commands.nonEmpty) {
                    dh.foreach(row => {
                        val id = row.getString("command_id")
                        if (commands.nonEmpty && commands.contains(id)) {
                            row.set("command_text", commands(id))
                        }
                        if (args.nonEmpty && args.contains(id)) {
                            row.set("args", args(id))
                        }
                    })
                }
                dh.put(s"INSERT INTO qross_tasks_dags (job_id, task_id, record_time, command_id, command_text, args, upstream_ids) VALUES (?, ?, '$recordTime', ?, ?, ?, ?)")

                //upstream_ids
                if (dag != "") {
                    dh.get(s"SELECT id FROM qross_jobs_dags WHERE job_id=$jobId AND id NOT IN ($dag)")
                        .put(s"UPDATE qross_tasks_dags SET upstream_ids=REPLACE(upstream_ids, '(#id)', '') WHERE task_id=$taskId")
                }

                //update task status
                if (status == TaskStatus.READY) {
                    dh.set(s"UPDATE qross_tasks SET status='${TaskStatus.READY}', ready_time=NOW(), readiness=TIMESTAMPDIFF(SECOND, create_time, NOW()) WHERE id=$taskId")
                      .set(s"UPDATE qross_tasks_living SET status='${TaskStatus.READY}' WHERE task_id=$taskId")
                }
                else {
                    dh.set(s"UPDATE qross_tasks SET status='${TaskStatus.INITIALIZED}' WHERE id=$taskId")
                      .set(s"UPDATE qross_tasks_living SET status='${TaskStatus.READY}' WHERE task_id=$taskId")
                }
            }

            dh.close()

            TaskRecorder.of(jobId, taskId, recordTime).debug(s"Instant Task $taskId of job $jobId <$recordTime> has been created.")
        }

        if (delay > 0 && jobId  > 0 && taskId > 0) {
            TaskRecorder.of(jobId, taskId, recordTime).debug(s"Instant Task $taskId of job $jobId <$recordTime> will start after $delay seconds.")
            Timer.sleep({ if (delay > 60) 60 else delay } seconds)
        }

        Task(taskId, status).of(jobId).at(taskTime, recordTime)
    }

    def killTask(taskId: Long, recordTime: String, killer: Int = 0): String = {

        val ds = DataSource.QROSS

        val task = ds.executeDataRow(s"SELECT job_id, 0 AS record_id, status FROM qross_tasks WHERE id=$taskId AND record_time='$recordTime'")
                        .orElse(ds.executeDataRow(s"SELECT job_id, id AS record_id, status FROM qross_tasks_records WHERE task_id=$taskId AND record_time='$recordTime'"))

        if (Set[String](TaskStatus.INSTANT, TaskStatus.NEW, TaskStatus.INITIALIZED, TaskStatus.READY, TaskStatus.EXECUTING).contains(task.getString("status"))) {

            val recordId = task.getLong("record_id")
            val jobId = task.getInt("job_id")

            val actions = ds.executeSingleList[Long](s"SELECT id FROM qross_tasks_dags WHERE task_id=$taskId AND status='running' AND record_time='$recordTime'")
            actions.foreach(action => {
                if (EXECUTING.contains(action)) {
                    TO_BE_KILLED += action -> killer
                }
            })
            //没有正在运行的 action 时只更新状态
            var warn = ""
            if (actions.isEmpty) {
                if (recordId == 0) {
                    ds.executeNonQuery(s"UPDATE qross_tasks SET status='${TaskStatus.INTERRUPTED}', killer=$killer WHERE id=$taskId AND status IN ('${TaskStatus.NEW}', '${TaskStatus.INITIALIZED}', '${TaskStatus.EXECUTING}', '${TaskStatus.READY}')")
                    warn = "Task is not yet executing, only status has been updated. Task won't go on."
                }
                else {
                    ds.executeNonQuery(s"UPDATE qross_tasks SET status='${TaskStatus.INTERRUPTED}', killer=$killer WHERE id=$recordId AND status IN ('${TaskStatus.NEW}', '${TaskStatus.INITIALIZED}', '${TaskStatus.EXECUTING}', '${TaskStatus.READY}')")
                    warn = "Task maybe get stuck, only status has been updated."
                }
                ds.executeNonQuery(s"DELETE FROM qross_tasks_living WHERE task_id=$taskId AND record_time='$recordTime'")

                TaskRecorder.of(jobId, taskId, recordTime).warn(s"Task $taskId of Job $jobId <$recordTime> has been KILLED, but $warn").collect().dispose()

            }

            ds.close()

            s"""{ "actions": [${actions.mkString(", ")}], "warn": "$warn" }"""
        }
        else {
            ds.close()
            s"""{ "actions": [], "warn": "Task is not active." }"""
        }
    }

    def killAction(actionId: Long, killer: Int = 0): String = {
        val  executor = DataSource.QROSS.querySingleValue(s"SELECT node_address FROM qross_tasks WHERE id=(SELECT task_id FROM qross_tasks_dags WHERE id=$actionId AND status='running')").asText("")
        if (executor == Keeper.NODE_ADDRESS) {
            if (EXECUTING.contains(actionId)) {
                TO_BE_KILLED += actionId -> killer
                s"""{ "action": [$actionId] }"""
            }
            else {
                """{ "action": [] }"""
            }
        }
        else if (executor.nonEmpty) {
            try {
                Http.PUT(s"""http://$executor/action/kill/$actionId?token=${Global.KEEPER_HTTP_TOKEN}&killer=$killer""").request()
            }
            catch {
                case e: java.net.ConnectException =>
                    Qross.disconnect(executor, e)
                    s"""{ "actions": [], "exception": "${e.getReferMessage.replace("\"", "'")}" }"""
                case e: Exception => e.printStackTrace() //e.printReferMessage()
                    s"""{ "actions": [], "exception": "${e.getReferMessage.replace("\"", "'")}" }"""
            }
        }
        else {
            """{ "action": [] }"""
        }
    }

    def restartTask(taskId: Long, option: String, starter: Int = 0): Task = {

        //Reset task status to RESTARTING in master
        //Reset action status to WAITING
        //Return status: initialized or ready

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
        dh.get(s"SELECT job_id, node_address, task_time, record_time, start_mode, starter, status, killer, ready_time, waiting_times, start_time, finish_time, readiness, latency, spent FROM qross_tasks WHERE id=$taskId")
            .put(s"INSERT INTO qross_tasks_records (job_id, node_address, task_id, record_time, start_mode, starter, status, killer, ready_time, waiting_times, start_time, finish_time, readiness, latency, spent) VALUES (#job_id, '#node_address', $taskId, '#record_time', '#start_mode', #starter, '#status', #killer, &ready_time, #waiting_times, &start_time, &finish_time, #readiness, #latency, #spent)")

        val row = dh.firstRow

        val jobId = row.getInt("job_id")
        val taskTime = row.getString("task_time")
        val prevRecordTime = row.getString("record_time")
        val recordTime = DateTime.now.toString
        val address = row.getString("node_address")

        var status = TaskStatus.READY

        restartMethod match {
            case WHOLE =>
                //generate before and after dependencies
                dh.get(s"""SELECT $jobId AS job_id, $taskId AS task_id, '$taskTime' AS task_time, '$recordTime' AS record_time, id AS dependency_id, dependency_moment, dependency_type, dependency_label, dependency_content, dependency_option FROM qross_jobs_dependencies WHERE job_id=$jobId AND enabled='yes'""")
                    .cache("dependencies")
                    .generateDependencies()

                //generate dags
                dh.get(s"SELECT A.job_id, $taskId AS task_id, A.id AS command_id, IF(B.id IS NULL, A.command_text, B.command_logic) AS command_text, A.args, A.upstream_ids FROM qross_jobs_dags A LEFT JOIN qross_commands_templates B ON A.template_id=B.id WHERE A.job_id=$jobId AND A.enabled='yes'")
                    .put(s"INSERT INTO qross_tasks_dags (job_id, task_id, record_time, command_id, command_text, args, upstream_ids) VALUES (?, ?, '$recordTime', ?, ?, ?, ?)")

                dh.openCache()
                    .get("SELECT task_id FROM dependencies WHERE dependency_moment='before'")
                if (dh.nonEmpty) {
                    status = TaskStatus.INITIALIZED
                }

            case PARTIAL =>
                dh.get(s"SELECT A.job_id, $taskId AS task_id, A.id AS command_id, IF(B.id IS NULL, A.command_text, B.command_logic) AS command_text, A.args, A.upstream_ids FROM qross_jobs_dags A LEFT JOIN qross_commands_templates ON A.template_id=B.id WHERE A.job_id=$jobId AND A.enabled='yes' AND A.id IN ($option)")
                    .put(s"INSERT INTO qross_tasks_dags (job_id, task_id, record_time, command_id, command_text, args, upstream_ids) VALUES (?, ?, '$recordTime', ?, ?, ?, ?)")
                .get(s"SELECT id FROM qross_jobs_dags WHERE job_id=$jobId AND enabled='yes' AND id NOT IN ($option)")
                    .put(s"UPDATE qross_tasks_dags SET upstream_ids=REPLACE(upstream_ids, '(#id)', '') WHERE task_id=$taskId AND record_time='$recordTime")

            case EXCEPTIONAL =>
                //generate "after" dependencies
                dh.get(s"SELECT $jobId AS job_id, $taskId AS task_id, '$taskTime' AS task_time, '$recordTime' AS record_time, id AS dependency_id, dependency_moment, dependency_type, dependency_label, dependency_content, dependency_option FROM qross_jobs_dependencies WHERE job_id=$jobId AND enabled='yes' AND dependency_moment='after'")
                    .generateDependencies()

                dh.get(s"SELECT IFNULL(GROUP_CONCAT(command_id), 0) AS command_ids FROM qross_tasks_dags WHERE task_id=$taskId AND record_time='$prevRecordTime' AND status IN ('exceptional', 'overtime', 'waiting')")
                    .pass(s"SELECT A.job_id, $taskId AS task_id, A.id AS command_id, IF(B.id IS NULL, A.command_text, B.command_logic) AS command_text, A.args, A.upstream_ids FROM qross_jobs_dags A LEFT JOIN qross_commands_templates B ON A.template_id=B.id WHERE A.job_id=$jobId AND A.enabled='yes' AND A.id IN (#command_ids)")
                        .put(s"INSERT INTO qross_tasks_dags (job_id, task_id, record_time, command_id, command_text, args, upstream_ids) VALUES (?, ?, '$recordTime', ?, ?, ?, ?)")
                    .get(s"SELECT command_id FROM qross_tasks_dags WHERE task_id=$taskId AND record_time='$prevRecordTime' AND status='done'")
                        .put(s"UPDATE qross_tasks_dags SET upstream_ids=REPLACE(upstream_ids, '(#command_id)', '') WHERE task_id=$taskId AND record_time='$recordTime'")
        }

        //update task
        dh.openQross()
            .set(s"UPDATE qross_tasks SET status='$status', start_mode='$restartMode', starter=$starter, record_time='$recordTime', readiness=NULL, latency=NULL, spent=NULL, ready_time=NULL, start_time=NULL, finish_time=NULL WHERE id=$taskId")
            .set(s"INSERT INTO qross_tasks_living (job_id, task_id, task_time, record_time, status, start_mode) VALUES ($jobId, $taskId, '$taskTime', '$recordTime', '$status', '$restartMode') ON DUPLICATE KEY UPDATE status='$status'")
            .set(s"INSERT INTO qross_tasks_stats (node_address, moment, created_tasks) VALUES ('$address', '${recordTime.dropRight(5) + "00:00"}', 1) ON DUPLICATE KEY UPDATE created_tasks=created_tasks+1")

        TaskRecorder.of(jobId, taskId, recordTime).debug(s"Task $taskId of job $jobId <$recordTime> restart with option $option.")

        dh.close()

        Task(taskId, status).of(jobId).in(address).at(taskTime, recordTime)
    }

    //distribute to every nodes to restart
    def restartAll(taskIds: String, option: String, starter: Int, producer: ActorSelection): String = {

        val nodes = Qross.availableNodes
        val result = new mutable.ArrayBuffer[String]()
        taskIds.split(",")
            .foreach(taskId => {
                var node = nodes.freest()
                val address = Keeper.NODE_ADDRESS

                if (node == address) {
                    val task = restartTask(taskId.toLong, option, starter)
                    producer ! task
                    result += s"""{"id":${task.id},"status":"${task.status}","recordTime":"${task.recordTime}"}"""
                }
                else {
                    breakable {
                        while (node != address) {
                            try {
                                result += Http.PUT(s"""http://$node/task/restart/$taskId?token=${Global.KEEPER_HTTP_TOKEN}&option=$option&starter=$starter""").request()
                                break
                            }
                            catch {
                                case e: java.net.ConnectException =>
                                    Qross.disconnect(node, e)
                                    nodes -= node //remove disconnected node
                                    node = nodes.freest()
                                case e: Exception => e.printStackTrace() //e.printReferMessage()
                            }
                        }
                    }

                    if (node == address) {
                        val task = restartTask(taskId.toLong, option, starter)
                        producer ! task
                        result += s"""{"id":${task.id},"status":"${task.status}","recordTime":"${task.recordTime}"}"""
                    }
                }
            })

        "[" + result.mkString(",") + "]"
    }

    //restart a task from a node in job flow
    def restartJobFlow(jobId: Int, taskId: Long, taskTime: String, excluding: Int*): String = {

        val dependencies = new mutable.HashMap[Int, mutable.ArrayBuffer[JobDependency]]()
        val closed = excluding.toSet
        val tasks = mutable.ArrayBuffer[Long](taskId)

        val ds = DataSource.QROSS

        ds.executeDataTable("SELECT job_id, dependency_label, dependency_content FROM qross_jobs_dependencies WHERE dependency_type='task' AND enabled='yes'")
            .foreach(row => {
                val upstreamJobId = row.getInt("dependency_label")
                if (!dependencies.contains(upstreamJobId)) {
                    dependencies += upstreamJobId -> new mutable.ArrayBuffer[JobDependency]()
                }
                dependencies(upstreamJobId) += new JobDependency(row.getInt("job_id"), row.getString("dependency_content"))
            }).clear()

        //breadth first
        if (dependencies.contains(jobId)) {
            var downstreamJobs = dependencies(jobId).map(_.setUpstreamTaskTime(taskTime))
            while (downstreamJobs.nonEmpty) {
                downstreamJobs = downstreamJobs.flatMap(downstream => {
                    val taskTime = downstream.getTaskTime
                    tasks += ds.executeSingleValue(s"SELECT id FROM qross_tasks WHERE job_id=${downstream.jobId} AND task_time=$taskTime").asInteger(0)
                    if (dependencies.contains(downstream.jobId) && !closed.contains(downstream.jobId)) {
                        dependencies(downstream.jobId).map(_.setUpstreamTaskTime(taskTime))
                    }
                    else {
                        new mutable.ArrayBuffer[JobDependency]()
                    }
                })
            }
        }

        ds.close()

        tasks.mkString(",")
    }

    //TaskChecker
    def checkTaskDependencies(task: Task): Boolean = {

        val taskId = task.id
        val jobId = task.jobId
        val recordTime = task.recordTime

        val dh = DataHub.QROSS

        //update task status to ready if all dependencies are ready
        val affected = dh.set(s"UPDATE qross_tasks SET status='${TaskStatus.READY}', ready_time=NOW(), readiness=TIMESTAMPDIFF(SECOND, create_time, NOW()) WHERE id=$taskId AND NOT EXISTS (SELECT task_id FROM qross_tasks_dependencies WHERE task_id=$taskId AND record_time='$recordTime' AND dependency_moment='before' AND ready='no')")
        if (affected > 0) {
            dh.set(s"UPDATE qross_tasks_living SET status='${TaskStatus.READY}' WHERE task_id=$taskId")
        }

        //check dependencies
        dh.openQross()
            .get(
                s"""SELECT A.id, A.job_id, A.task_id, A.dependency_type, A.dependency_label, A.dependency_content, A.dependency_option, A.ready, B.task_time, A.record_time
                    FROM (SELECT id, job_id, task_id, record_time, dependency_type, dependency_label, dependency_content, dependency_option, ready FROM qross_tasks_dependencies WHERE task_id=$taskId AND record_time='$recordTime' AND dependency_moment='before' AND ready='no') A
                    INNER JOIN (SELECT id, task_time FROM qross_tasks where id=$taskId) B ON A.task_id=B.id""")
            .foreach(row => {
                if (TaskDependency.check(row) == "yes") {
                    row.set("ready", "yes")
                    TaskRecorder.of(jobId, taskId, recordTime).log(s"Task $taskId of job $jobId <$recordTime> dependency ${row.getLong("id")} is ready.")
                }
            }).cache("dependencies")

        //update status and others after checking
        dh.openCache()
            .get("SELECT id FROM dependencies WHERE ready='yes'")
                .put("UPDATE qross_tasks_dependencies SET ready='yes' WHERE id=#id")
            .get("SELECT id FROM dependencies WHERE ready='no'")
                .put("UPDATE qross_tasks_dependencies SET retry_times=retry_times+1 WHERE id=#id")
            .get("SELECT task_id FROM dependencies WHERE ready='no'")

        val status = {
            if (dh.isEmpty) {
                dh.openQross()
                    .set(s"UPDATE qross_tasks SET status='${TaskStatus.READY}', ready_time=NOW(), readiness=TIMESTAMPDIFF(SECOND, create_time, NOW()) WHERE id=$taskId")
                    .set(s"UPDATE qross_tasks_living SET status='${TaskStatus.READY}' WHERE id=$taskId")
                    .clear()
                TaskStatus.READY
            }
            else {
                dh.clear()
                TaskStatus.INITIALIZED
            }
        }

        dh.openQross()
            .set(s"INSERT INTO qross_tasks_stats (node_address, moment, checked_tasks) VALUES ('${Keeper.NODE_ADDRESS}', '${DateTime.now.getTockValue}', 1) ON DUPLICATE KEY UPDATE checked_tasks=checked_tasks+1")

        if (status == TaskStatus.INITIALIZED)  {
            //check for checking limit
            dh.openQross()
                .get(s"""SELECT A.task_id, A.retry_times, B.retry_limit, A.job_id
                        FROM (SELECT task_id, job_id, dependency_id, retry_times FROM qross_tasks_dependencies WHERE task_id=$taskId AND record_time='$recordTime' AND dependency_moment='before' AND ready='no') A
                        INNER JOIN (SELECT id, retry_limit FROM qross_jobs_dependencies WHERE job_id=$jobId AND enabled='yes') B ON A.dependency_id=B.id WHERE B.retry_limit>0 AND A.retry_times>=B.retry_limit""")

            if (dh.nonEmpty) {
                //TaskStatus.CHECKING_LIMIT
                dh.set(s"UPDATE qross_tasks SET node_address='${Keeper.NODE_ADDRESS}', status='${TaskStatus.CHECKING_LIMIT}' WHERE id=$taskId")
                    .set(s"DELETE FROM qross_tasks_living WHERE task_id=$taskId")
                    .set(s"INSERT INTO qross_tasks_abnormal (job_id, task_id, task_time, record_time, status) VALUES ($jobId, $taskId, '${task.taskTime}', '$recordTime', '${TaskStatus.CHECKING_LIMIT}')")
                    .set(s"INSERT INTO qross_tasks_stats (node_address, moment, completed_tasks, exceptional_tasks) VALUES ('${Keeper.NODE_ADDRESS}', '${DateTime.now.getTockValue}', 1, 1) ON DUPLICATE KEY UPDATE completed_tasks=completed_tasks+1, exceptional_tasks=exceptional_tasks+1")
                    .set(s"UPDATE qross_jobs SET unchecked_exceptional_tasks=IFNULL((SELECT GROUP_CONCAT(unchecked_exceptional_status ORDER BY unchecked_exceptional_status) AS exceptional_status FROM (SELECT CONCAT(`status`, ':', COUNT(0)) AS unchecked_exceptional_status FROM qross_tasks_abnormal WHERE job_id=$jobId AND checked='no' GROUP BY `status`) T), '') WHERE id=$jobId;")

                TaskRecorder.of(jobId, taskId, recordTime).warn(s"Task $taskId of job $jobId <$recordTime> reached upper limit of checking limit.")

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
                        .get("SELECT A.*, B.event_value AS value, B.event_function, B.event_limit, '' AS event_result, B.event_name FROM task_info A INNER JOIN events B ON A.job_id=B.job_id AND INSTR(B.event_function, 'CUSTOM_')>0 AND INSTR(B.event_limit, A.start_mode)>0")
                            .fireCustomEvent(TaskStatus.CHECKING_LIMIT)
                        .get("SELECT A.*, B.event_value AS delay, B.event_function, B.event_limit, '' AS to_be_start_time, B.event_name FROM task_info A INNER JOIN events B ON A.job_id=B.job_id AND B.event_function='RESTART_CHECKING_AFTER' AND (B.event_option=0 OR B.current_option<B.event_option) AND INSTR(B.event_limit, A.start_mode)>0")
                            .restartTask(TaskStatus.CHECKING_LIMIT)
                        .get("SELECT A.*, B.event_option AS script_type, B.event_value AS script, B.event_limit, '' AS event_result, B.event_name, B.event_function FROM task_info A INNER JOIN events B ON A.job_id=B.job_id AND B.event_function='EXECUTE_SCRIPT' AND INSTR(B.event_limit, A.start_mode)>0")
                            .runScript(TaskStatus.CHECKING_LIMIT)
                }

                //collect logs and end tasks
                TaskRecorder.of(jobId, taskId, recordTime).collect().dispose()
            }
            else {
                TaskRecorder.of(jobId, taskId, recordTime).log(s"Task $taskId of job $jobId <$recordTime> status is not ready after pre-dependencies checking.")
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
                    .get(s"SELECT A.task_id, A.job_id, A.title, A.owner, A.task_time, A.record_time, B.event_value AS value, B.event_function, B.event_limit, '' AS event_result, B.event_name FROM task_info A INNER JOIN events B ON A.job_id=B.job_id AND INSTR(B.event_function, 'CUSTOM_')>0 AND INSTR(B.event_limit, A.start_mode)>0")
                        .fireCustomEvent(TaskStatus.READY)
                    .get(s"SELECT A.task_id, A.job_id, A.title, A.owner, A.task_time, A.record_time, B.event_option AS script_type, B.event_value AS script, B.event_limit, '' AS event_result, B.event_name, B.event_function FROM task_info A INNER JOIN events B ON A.job_id=B.job_id AND B.event_function='EXECUTE_SCRIPT' AND INSTR(B.event_limit, A.start_mode)>0")
                        .runScript(TaskStatus.READY)
            }

            TaskRecorder.of(jobId, taskId, recordTime).log(s"Task $taskId of job $jobId <$recordTime> status is ready after pre-dependencies checking.")
        }

        dh.close()

        status == TaskStatus.READY
    }

    def checkTasksStatus(tick: String): mutable.ArrayBuffer[Task] = {

        val address = Keeper.NODE_ADDRESS
        val dh = DataHub.QROSS

        val tasks = new mutable.ArrayBuffer[Task]()
        val locked: Boolean = dh.executeNonQuery(s"UPDATE qross_keeper_locks SET node_address='$address', tick='$tick', lock_time=NOW() WHERE lock_name='CHECK-TASKS' AND tick<>'$tick'") == 1
        if (locked) {
            //tasks to be restart
            dh.get(s"SELECT id, job_id, task_id, task_time, record_time, status FROM qross_tasks_to_be_start WHERE to_be_start_time='$tick'")
            if (dh.nonEmpty) {
                dh.put(s"DELETE FROM qross_tasks_to_be_start WHERE id=#id")
                    .cache("tasks")
                    .openCache()
                    .get(s"SELECT task_id FROM tasks WHERE status='${TaskStatus.CHECKING_LIMIT}'")
                        .put(s"UPDATE qross_tasks_dependencies SET retry_times=0 WHERE task_id=#task_id")
                        .put(s"UPDATE qross_tasks SET status='${TaskStatus.INITIALIZED}', start_mode='auto_restart' WHERE id=#task_id")
                        .put(s"INSERT INTO qross_tasks_living (job_id, task_id, task_time, record_time, status, start_mode) VALUES (#job_id, #task_id, '#task_time', '#record_time', '${TaskStatus.INITIALIZED}', 'auto_restart') ON DUPLICATE KEY UPDATE status='${TaskStatus.INITIALIZED}'")
                        .set(s"INSERT INTO qross_tasks_stats (node_address, moment, created_tasks) VALUES ('$address', '${tick.dropRight(5) + "00:00"}', ${dh.AFFECTED_ROWS_OF_LAST_PUT}) ON DUPLICATE KEY UPDATE created_tasks=created_tasks+${dh.AFFECTED_ROWS_OF_LAST_PUT}")
                    .get(s"SELECT task_id FROM tasks WHERE status='${TaskStatus.INCORRECT}'")
                        .foreach(row => {
                            tasks += restartTask(row.getLong("task_id"), "^WHOLE")
                        })
                        .clear()
                    .get(s"SELECT task_id FROM tasks WHERE status='${TaskStatus.FAILED}' OR status='${TaskStatus.TIMEOUT}' OR status='${TaskStatus.INTERRUPTED}'")
                        .foreach(row => {
                            tasks += restartTask(row.getLong("task_id"), "^EXCEPTIONAL")
                        })
            }
            dh.clear() //clear data struct too

            //stuck tasks - executing tasks but no running actions
            //get executing tasks
            dh.openQross()
                .get(
                    s"""select id, job_id, task_id, command_id, upstream_ids, record_time, TIMESTAMPDIFF(SECOND, update_time, NOW()) AS span, status
                                FROM qross_tasks_dags WHERE task_id IN (SELECT task_id FROM qross_tasks_living WHERE status='${TaskStatus.EXECUTING}')""")
                .cache("dags")

            if (dh.nonEmpty) {
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
                        .put(s"UPDATE qross_tasks SET status='${TaskStatus.READY}', ready_time=NOW(), readiness=TIMESTAMPDIFF(SECOND, create_time, NOW()) WHERE id=#task_id AND status='${TaskStatus.EXECUTING}'")
                        .put(s"UPDATE qross_tasks_living SET status='${TaskStatus.READY}' WHERE task_id=#task_id")
                        .put(s"UPDATE qross_stuck_records SET renewed='yes' where id=#id")
                }
            }
        }

        writeLineWithSeal("SYSTEM", s"<$address> TaskStarter beat!")
        dh.set(s"UPDATE qross_keeper_beats SET last_beat_time=NOW() WHERE node_address='$address' AND actor_name='TaskStarter'")

        dh.close()

        //task to be restart
        tasks
    }

    //TaskStarter - execute()
    def getTaskCommandsToExecute(task: Task): DataTable = synchronized {

        val dh = DataHub.QROSS

        val address = Keeper.NODE_ADDRESS
        val taskId = task.id
        val jobId = task.jobId
        val status = task.status
        val recordTime = task.recordTime
        val concurrentLimit = dh.executeSingleValue(s"SELECT concurrent_limit FROM qross_jobs WHERE id=$jobId").asInteger(1)

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

            if (concurrentLimit == 0 || dh.executeDataRow(s"SELECT COUNT(0) AS concurrent FROM qross_tasks_living WHERE job_id=$jobId AND status='${TaskStatus.EXECUTING}'").getInt("concurrent") < concurrentLimit) {
                //collect logs
                TaskRecorder.of(jobId, taskId, recordTime).collect()
                //ready to execute
                val map = dh.executeDataMap[String, Int](s"SELECT status, COUNT(0) AS amount FROM qross_tasks_dags WHERE task_id=$taskId AND record_time='$recordTime' GROUP BY status")
                if (map.isEmpty) {
                    //quit if no commands to execute
                    dh.set(s"UPDATE qross_tasks SET status='${TaskStatus.NO_COMMANDS}' WHERE id=$taskId")
                        .set(s"DELETE FROM qross_tasks_living WHERE task_id=$taskId")

                    TaskRecorder.of(jobId, taskId, recordTime).warn(s"Task $taskId of job $jobId <$recordTime> has been closed because no commands exists on task ready.")
                }
                else if (map.contains(ActionStatus.EXCEPTIONAL) || map.contains(ActionStatus.OVERTIME)) {
                    //restart task at next minute if exceptional or overtime
                    dh.set(s"UPDATE qross_tasks SET status='${TaskStatus.INTERRUPTED}' WHERE id=$taskId")
                        .set(s"DELETE FROM qross_tasks_living WHERE task_id=$taskId")
                        .set(s"INSERT INTO qross_tasks_to_be_start (job_id, task_id, task_time, record_time, status, to_be_start_time) VALUES ($jobId, $taskId, '${task.taskTime}', '$recordTime', '${TaskStatus.INTERRUPTED}', '${DateTime.now.plusMinutes(1).getTickValue}')")
                    TaskRecorder.of(jobId, taskId, recordTime).warn(s"Task $taskId of job $jobId <$recordTime> restart because EXCEPTIONAL commands exists on task ready.")
                }
                else if (map.contains(ActionStatus.WAITING) || map.contains(ActionStatus.QUEUEING) || map.contains(ActionStatus.RUNNING)) {
                    //executing
                    dh.set(s"UPDATE qross_tasks SET node_address='$address', status='${TaskStatus.EXECUTING}', start_time=NOW(), latency=TIMESTAMPDIFF(SECOND, ready_time, NOW()) WHERE id=$taskId")
                      .set(s"UPDATE qross_tasks_living SET status='${TaskStatus.EXECUTING}', node_address='$address' WHERE task_id=$taskId")
                      .set(s"INSERT INTO qross_tasks_stats (node_address, moment, executed_tasks) VALUES ('$address', '${DateTime.now.getTockValue}', 1) ON DUPLICATE KEY UPDATE executed_tasks=executed_tasks+1")

                    TaskRecorder.of(jobId, taskId, recordTime).log(s"Task $taskId of job $jobId <$recordTime> start executing on task ready.")
                }
                else {
                    //finished
                    dh.set(s"UPDATE qross_tasks SET status='${TaskStatus.SUCCESS}' WHERE id=$taskId")
                      .set(s"DELETE FROM qross_tasks_living WHERE task_id=$taskId")
                      .set(s"INSERT INTO qross_tasks_stats (node_address, moment, completed_tasks) VALUES ('$address', '${DateTime.now.getTockValue}', 1, 1) ON DUPLICATE KEY UPDATE completed_tasks=completed_tasks+1")
                    TaskRecorder.of(jobId, taskId, recordTime).warn(s"Task $taskId of job $jobId <$recordTime> changes status to '${TaskStatus.SUCCESS}' because all commands has been executed on task ready.")
                }
            }
            else {
                TaskRecorder.of(jobId, taskId, recordTime).warn(s"Concurrent reach upper limit of Job $jobId for Task $taskId <$recordTime> on task ready.")

                dh.set(s"UPDATE qross_tasks SET waiting_times=waiting_times+1 WHERE id=$taskId")
                    .get(s"SELECT A.id AS task_id, A.job_id, A.waiting_times, B.waiting_overtime FROM (SELECT id, job_id, waiting_times FROM qross_tasks WHERE id=$taskId) A INNER JOIN (SELECT waiting_overtime FROM qross_jobs WHERE id=$jobId) B WHERE B.waiting_overtime>0 AND A.waiting_times>=B.waiting_overtime")

                if (dh.nonEmpty) {
                    //TaskStatus.CHECKING_LIMIT
                    dh.set(s"UPDATE qross_tasks SET node_address='${Keeper.NODE_ADDRESS}', status='${TaskStatus.WAITING_LIMIT}' WHERE id=$taskId")
                        .set(s"DELETE FROM qross_tasks_living WHERE task_id=$taskId")
                        .set(s"INSERT INTO qross_tasks_abnormal (job_id, task_id, task_time, record_time, status) VALUES ($jobId, $taskId, '${task.taskTime}', '$recordTime', '${TaskStatus.WAITING_LIMIT}')")
                        .set(s"INSERT INTO qross_tasks_stats (node_address, moment, completed_tasks, exceptional_tasks) VALUES ('$address', '${DateTime.now.getTockValue}', 1, 1) ON DUPLICATE KEY UPDATE completed_tasks=completed_tasks+1, exceptional_tasks=exceptional_tasks+1")
                        .set(s"UPDATE qross_jobs SET unchecked_exceptional_tasks=IFNULL((SELECT GROUP_CONCAT(unchecked_exceptional_status ORDER BY unchecked_exceptional_status) AS exceptional_status FROM (SELECT CONCAT(`status`, ':', COUNT(0)) AS unchecked_exceptional_status FROM qross_tasks_abnormal WHERE job_id=$jobId AND checked='no' GROUP BY `status`) T), '') WHERE id=$jobId;")

                    TaskRecorder.of(jobId, taskId, recordTime).warn(s"Task $taskId of job $jobId <$recordTime> reached upper limit of waiting limit.")

                    //update status if reach upper limit
                    dh.join(
                        s"""SELECT B.task_id, A.title, A.owner, B.job_id, B.task_time, B.record_time, B.start_mode
                            FROM (SELECT id, title, owner FROM qross_jobs WHERE id=$jobId) A
                            INNER JOIN (SELECT id AS task_id, job_id, task_time, record_time, start_mode FROM qross_tasks WHERE id=$taskId) B ON A.id=B.job_id""", "job_id" -> "job_id")
                        .cache("task_info")

                    //execute onTaskCheckingLimit event
                    dh.get(
                        s"""SELECT A.job_id, A.event_name, A.event_function, A.event_limit, A.event_value, A.event_option, IFNULL(B.current_option, 0) AS current_option FROM
                                            (SELECT job_id, event_name, event_function, event_limit, event_value, event_option FROM qross_jobs_events WHERE job_id=$jobId AND enabled='yes' AND event_name='onTaskWaitingLimit') A
                                                LEFT JOIN
                                            (SELECT job_id, event_function, COUNT(0) AS current_option FROM qross_tasks_events WHERE task_id=$taskId AND event_name='onTaskWaitingLimit' GROUP BY job_id, event_function) B
                                            ON A.job_id=B.job_id AND A.event_function=B.event_function""")

                    if (dh.nonEmpty) {
                        dh.cache("events")
                        //onTaskCheckingLimit
                        dh.openCache()
                            .get("SELECT A.*, B.event_value AS receivers, B.event_limit, '' AS event_result, B.event_name, B.event_function FROM task_info A INNER JOIN events B ON A.job_id=B.job_id WHERE event_function='SEND_MAIL_TO' AND INSTR(B.event_limit, A.start_mode)>0")
                                .sendEmail(TaskStatus.WAITING_LIMIT)
                            .get("SELECT A.*, B.event_value AS api, B.event_option AS method, B.event_limit, '' AS event_result, B.event_name, B.event_function FROM task_info A INNER JOIN events B ON A.job_id=B.job_id AND B.event_function='REQUEST_API' AND INSTR(B.event_limit, A.start_mode)>0")
                                .requestApi(TaskStatus.WAITING_LIMIT)
                            .get("SELECT A.*, B.event_value AS value, B.event_function, B.event_limit, '' AS event_result, B.event_name FROM task_info A INNER JOIN events B ON A.job_id=B.job_id AND INSTR(B.event_function, 'CUSTOM_')>0 AND INSTR(B.event_limit, A.start_mode)>0")
                                .fireCustomEvent(TaskStatus.WAITING_LIMIT)
                            .get("SELECT A.*, B.event_value AS delay, B.event_function, B.event_limit, '' AS to_be_start_time, B.event_name FROM task_info A INNER JOIN events B ON A.job_id=B.job_id AND B.event_function='RESTART_CHECKING_AFTER' AND (B.event_option=0 OR B.current_option<B.event_option) AND INSTR(B.event_limit, A.start_mode)>0")
                                .restartTask(TaskStatus.WAITING_LIMIT)
                            .get("SELECT A.*, B.event_option AS script_type, B.event_value AS script, B.event_limit, '' AS event_result, B.event_name, B.event_function FROM task_info A INNER JOIN events B ON A.job_id=B.job_id AND B.event_function='EXECUTE_SCRIPT' AND INSTR(B.event_limit, A.start_mode)>0")
                                .runScript(TaskStatus.WAITING_LIMIT)
                    }

                    //collect logs and end tasks
                    TaskRecorder.of(jobId, taskId, recordTime).collect().dispose()
                }
            }
        }

        dh.get(
            s"""SELECT A.action_id, A.job_id, A.task_id, A.command_id, B.task_time, B.record_time, B.start_mode, C.command_type, A.command_text, A.args, C.overtime, C.retry_limit, D.job_type, D.title, D.owner
                         FROM (SELECT id AS action_id, job_id, task_id, command_id, command_text, args FROM qross_tasks_dags WHERE task_id=$taskId AND record_time='$recordTime' AND status='${ActionStatus.WAITING}' AND upstream_ids='') A
                         INNER JOIN (SELECT id, task_time, record_time, start_mode FROM qross_tasks WHERE id=$taskId AND status='${TaskStatus.EXECUTING}') B ON A.task_id=B.id
                         INNER JOIN (SELECT id, command_type, overtime, retry_limit FROM qross_jobs_dags WHERE job_id=$jobId) C ON A.command_id=C.id
                         INNER JOIN (SELECT id, title, job_type, owner FROM qross_jobs WHERE id=$jobId) D ON A.job_id=D.id""")

        writeDebugging(s"Task $taskId of $jobId get ${dh.count()} commands to execute.")

        //prepare to run command - start time point
        if (dh.nonEmpty) {
            dh.put(s"UPDATE qross_tasks_dags SET start_time=NOW(), lagged=TIMESTAMPDIFF(SECOND, record_time, NOW()), status='${ActionStatus.QUEUEING}' WHERE id=#(action_id)")
        }

        val executable = dh.takeOut()

        dh.close()

        executable
    }

    //TaskExecutor
    def executeTaskCommand(taskCommand: DataRow): Task = {

        //在解析时（运行前）是否发生了错误
        var error = false
        var path = ""

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

        EXECUTING += actionId

        val logger = TaskRecorder.of(jobId, taskId, recordTime)

        var commandText = taskCommand.getString("command_text").trim()
        val args = taskCommand.getString("args", "")

        taskCommand.remove("args")
        taskCommand.remove("overtime")
        taskCommand.remove("retry_limit")
        taskCommand.remove("job_type")
        taskCommand.remove("command_type")
        taskCommand.remove("command_text")

        try {
            taskCommand.combine(args)
        }
        catch {
            case e: Exception =>
                logger.err("Error occurred in arguments, please check.", commandId, actionId)
                e.getFullMessage.forEach(message => {
                    logger.err(message, commandId, actionId)
                })
                error = true
        }

        if (commandType != "pql") {
            //在Keeper中处理的好处是在命令的任何地方都可嵌入表达式
            try {
                commandText = PQL.openEmbedded(commandText).asCommandOf(jobId).place(taskCommand).run().asInstanceOf[String] //按PQL计算, 支持各种PQL嵌入式表达式, 但不保留引号
            }
            catch {
                case e: Exception =>
                    logger.err("Error occurred in command text, please check.", commandId, actionId)
                    e.getFullMessage.forEach(message => {
                        logger.err(message, commandId, actionId)
                    })
                    error = true
            }
        }

        val dh = DataHub.QROSS

        dh.set(s"UPDATE qross_tasks_dags SET status='${if (error) ActionStatus.WRONG else ActionStatus.RUNNING}', command_text=?, args=?, run_time=NOW(), waiting=TIMESTAMPDIFF(SECOND, start_time, NOW()) WHERE id=$actionId", commandText, taskCommand.toString())

        if (commandType == "pql") {
            //命令变为 worker
            commandText = Global.JAVA_BIN_HOME + s"java -Dfile.encoding=${Global.CHARSET} -jar ${Global.QROSS_HOME}qross-worker-${Global.QROSS_VERSION}.jar -job $jobId -task $actionId"
        }
        else if (commandType == "shell") {
            //auto add environment variable to ahead
            commandText.takeBeforeX($BLANK).toLowerCase() match {
                case "java" => commandText = Global.JAVA_BIN_HOME + "java " + commandText.takeAfterX($BLANK)
                case "pql" => commandText = Global.JAVA_BIN_HOME + s"java -Dfile.encoding=${Global.CHARSET} -jar ${Global.QROSS_HOME}qross-worker-${Global.QROSS_VERSION}.jar " + commandText.takeAfterX($BLANK)
                case "python2" => commandText = Global.PYTHON2_HOME + "python2 " + commandText.takeAfterX($BLANK)
                case "python3" => commandText = Global.PYTHON3_HOME + "python3 "+ commandText.takeAfterX($BLANK)
                case _ =>
            }

            //多行shell保存为文件 Qross_home/shell/actionId.py
            if (commandText.contains("\n")) {
                path = Global.QROSS_HOME + s"shell/$actionId.sh"
                new FileWriter(path, true).write(commandText).close()
                s"chmod +x $path".bash()
                commandText = path
            }
        }
        else if (commandType == "python") {
            commandText = PQL.openEmbedded(commandText).asCommandOf(jobId).place(taskCommand).run().asInstanceOf[String]
            //保存为文件 Qross_home/python/actionId.py
            path = Global.QROSS_HOME + s"python/$actionId.py"
            new FileWriter(path, true).write(commandText).close()
            commandText = Global.PYTHON3_HOME + "python3 " + path
        }

        var exitValue = 1
        var continue = false
        var retry = -1

        if (!error) {
            var killed = false

            logger.debug(s"START action $actionId - command $commandId of task $taskId - job $jobId <$recordTime>: $commandText", commandId, actionId)

            do {
                if (retry > 0) {
                    logger.debug(s"Action $actionId - command $commandId of task $taskId - job $jobId <$recordTime>: retry $retry of limit $retryLimit", commandId, actionId)
                }
                val start = System.currentTimeMillis()
                var timeout = false

                try {
                    val process = commandText.shell.run(
                            ProcessLogger(
                                out => logger.out(out, commandId, actionId),
                                err => logger.err(err, commandId, actionId))
                    )

                    while (process.isAlive()) {
                        //if timeout
                        if (overtime > 0 && (System.currentTimeMillis() - start) / 1000 > overtime) {
                            process.destroy() //kill it
                            timeout = true
                            logger.warn(s"Action $actionId - command $commandId of task $taskId - job $jobId <$recordTime> is TIMEOUT: $commandText", commandId, actionId)
                        }
                        else if (TO_BE_KILLED.contains(actionId)) {
                            //kill child process first
                            if (commandType == "shell") {
                                if (path != "") {
                                    Shell.end(path)
                                }
                                else {
                                    //仅支持单行sh文件命令 且参数中不能包含双引号
                                    """(^|;)(/[^;\s]+\.sh)""".r.findFirstMatchIn(commandText) match {
                                        case Some(m) =>
                                            val sh = commandText.substring(commandText.indexOf(m.group(2)))
                                            if (sh.contains("\"")) {
                                                Shell.end(sh.takeBefore("\""))
                                            }
                                            else {
                                                Shell.end(sh)
                                            }
                                        case None =>
                                    }
                                }
                            }
                            //kill it
                            process.destroy()
                            killed = true
                            logger.warn(s"Action $actionId - command $commandId of task $taskId - job $jobId <$recordTime> has been KILLED: $commandText", commandId, actionId)
                        }

                        Timer.sleep(1 seconds)
                    }

                    exitValue = process.exitValue()
                }
                catch {
                    case e: Exception =>
                        logger.err("ERROR: " + commandText, commandId, actionId)
                        e.printStackTrace()

                        val buf = new java.io.ByteArrayOutputStream()
                        e.printStackTrace(new java.io.PrintWriter(buf, true))
                        logger.err(buf.toString(), commandId, actionId)
                        buf.close()

                        logger.err(s"Action $actionId - command $commandId of task $taskId - job $jobId is exceptional: ${e.getMessage}", commandId, actionId)

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

            logger.debug(s"FINISH action $actionId - command $commandId of task $taskId - job $jobId with exitValue $exitValue and status ${if (exitValue == 0) "SUCCESS" else if (exitValue > 0) "FAILURE" else "TIMEOUT/INTERRUPTED"}", commandId, actionId)
        }

        if (path != "") {
            new File(path).delete()
        }

        val status = exitValue match {
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
                    dh.set(s"UPDATE qross_tasks SET finish_time=NOW(), spent=TIMESTAMPDIFF(SECOND, start_time, NOW()), status='${TaskStatus.FINISHED}' WHERE id=$taskId AND NOT EXISTS (SELECT id FROM qross_tasks_dags WHERE task_id=$taskId AND record_time='$recordTime' AND status!='${ActionStatus.DONE}')")

                    //只有所有Action执行完成才算完成
                    if (dh.AFFECTED_ROWS_OF_LAST_SET > 0) {
                        //check "after" dependencies
                        dh.set(s"UPDATE qross_tasks_living SET status='${TaskStatus.FINISHED}' WHERE task_id=$taskId")
                            .get(s"SELECT A.id, A.task_id, A.record_time, A.dependency_type, A.dependency_label, A.dependency_content, A.dependency_option, A.ready, B.task_time FROM qross_tasks_dependencies A INNER JOIN qross_tasks_living B ON A.job_id=B.job_id WHERE B.status='${TaskStatus.FINISHED}' AND A.task_id=$taskId AND A.record_time='$recordTime' AND A.dependency_moment='after' AND A.ready='no'")
                            .foreach(row => {
                                if (TaskDependency.check(row) == "yes") {
                                    row.set("ready", "yes")
                                }
                            }).put("UPDATE qross_tasks_dependencies SET ready='#ready' WHERE id=#id")

                        dh.get(s"SELECT id FROM qross_tasks_dependencies WHERE task_id=$taskId AND record_time='$recordTime' AND dependency_moment='after' AND ready='no'")

                        if (dh.nonEmpty) {
                            dh.clear()
                                .set(s"UPDATE qross_tasks SET status='${TaskStatus.INCORRECT}' WHERE id=$taskId AND status='${TaskStatus.FINISHED}'")
                                .set(s"INSERT INTO qross_tasks_abnormal (job_id, task_id, task_time, record_time, status) VALUES ($jobId, $taskId, '$taskTime', '$recordTime', '${TaskStatus.INCORRECT}')")
                            TaskStatus.INCORRECT
                        }
                        else {
                            dh.clear().set(s"UPDATE qross_tasks SET status='${TaskStatus.SUCCESS}' WHERE id=$taskId AND status='${TaskStatus.FINISHED}'")
                            TaskStatus.SUCCESS
                        }
                    }
                    else {
                        TaskStatus.EMPTY
                    }
                }
                else {
                    TaskStatus.EMPTY
                }
            //timeout
            case -1 =>
                dh.set(s"UPDATE qross_tasks_dags SET status='${ActionStatus.OVERTIME}', finish_time=NOW(), elapsed=TIMESTAMPDIFF(SECOND, run_time, NOW()), retry_times=$retry WHERE id=$actionId")
                    .set(s"UPDATE qross_tasks SET status='${TaskStatus.TIMEOUT}', finish_time=NOW(), spent=TIMESTAMPDIFF(SECOND, start_time, NOW()) WHERE id=$taskId")
                    .set(s"INSERT INTO qross_tasks_abnormal (job_id, task_id, task_time, record_time, status) VALUES ($jobId, $taskId, '$taskTime', '$recordTime', '${TaskStatus.TIMEOUT}')")
                TaskStatus.TIMEOUT
            //killed
            case -2 =>
                dh.set(s"UPDATE qross_tasks_dags SET status='${ActionStatus.KILLED}', finish_time=NOW(), elapsed=TIMESTAMPDIFF(SECOND, run_time, NOW()) WHERE id=$actionId")
                dh.set(s"UPDATE qross_tasks SET status='${TaskStatus.INTERRUPTED}', killer=${TO_BE_KILLED(actionId)}, finish_time=NOW(), spent=TIMESTAMPDIFF(SECOND, start_time, NOW()) WHERE id=$taskId")
                TO_BE_KILLED -= actionId
                TaskStatus.INTERRUPTED
            //failed
            case _ =>
                dh.set(s"UPDATE qross_tasks_dags SET status='${ActionStatus.EXCEPTIONAL}', finish_time=NOW(), elapsed=TIMESTAMPDIFF(SECOND, run_time, NOW()), retry_times=$retry WHERE id=$actionId")
                    .set(s"UPDATE qross_tasks SET finish_time=NOW(), spent=TIMESTAMPDIFF(SECOND, start_time, NOW()), status='${TaskStatus.FAILED}' WHERE id=$taskId")
                    .set(s"INSERT INTO qross_tasks_abnormal (job_id, task_id, task_time, record_time, status) VALUES ($jobId, $taskId, '$taskTime', '$recordTime', '${TaskStatus.FAILED}')")
                TaskStatus.FAILED
        }

        dh.set(s"DELETE FROM qross_tasks_living WHERE task_id=$taskId")

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
                    .get(s"SELECT A.*, B.event_value AS value, B.event_function, B.event_limit, '' AS event_result, B.event_name FROM task_info A INNER JOIN events B ON A.job_id=B.job_id AND INSTR(B.event_function, 'CUSTOM_')>0 AND INSTR(B.event_limit, A.start_mode)>0")
                        .fireCustomEvent(status)
                    .get("SELECT A.*, B.event_option AS script_type, B.event_value AS script, B.event_limit, '' AS event_result, B.event_name, B.event_function FROM task_info A INNER JOIN events B ON A.job_id=B.job_id AND B.event_function='EXECUTE_SCRIPT' AND INSTR(B.event_limit, A.start_mode)>0")
                        .runScript(status)
            }

            if (status != TaskStatus.SUCCESS) {
                dh.openQross()
                    .set(s"UPDATE qross_jobs SET unchecked_exceptional_tasks=IFNULL((SELECT GROUP_CONCAT(unchecked_exceptional_status ORDER BY unchecked_exceptional_status) AS exceptional_status FROM (SELECT CONCAT(`status`, ':', COUNT(0)) AS unchecked_exceptional_status FROM qross_tasks_abnormal WHERE job_id=$jobId AND checked='no' GROUP BY `status`) T), '') WHERE id=$jobId")
                    .set(s"INSERT INTO qross_tasks_stats (node_address, moment, completed_tasks, exceptional_tasks) VALUES ('${Keeper.NODE_ADDRESS}', '${DateTime.now.getTockValue}', 1, 1) ON DUPLICATE KEY UPDATE completed_tasks=completed_tasks+1, exceptional_tasks=exceptional_tasks+1")
            }
            else {
                //执行成功之后则去掉unchecked标记
                dh.openQross()
                    .set(s"UPDATE qross_tasks_abnormal SET checked='yes', mender=0 WHERE task_id=$taskId AND checked='no'")
                    .set(s"UPDATE qross_jobs SET unchecked_exceptional_tasks=IFNULL((SELECT GROUP_CONCAT(unchecked_exceptional_status ORDER BY unchecked_exceptional_status) AS exceptional_status FROM (SELECT CONCAT(`status`, ':', COUNT(0)) AS unchecked_exceptional_status FROM qross_tasks_abnormal WHERE job_id=$jobId AND checked='no' GROUP BY `status`) T), '') WHERE id=$jobId")
                    .set(s"INSERT INTO qross_tasks_stats (node_address, moment, completed_tasks) VALUES ('${Keeper.NODE_ADDRESS}', '${DateTime.now.getTockValue}', 1) ON DUPLICATE KEY UPDATE completed_tasks=completed_tasks+1")
            }
        }

        EXECUTING -= actionId

        //execute next ready Task of this Job
        val nextTask = {
            if (!continue && jobType != JobType.ENDLESS) {
                dh.executeDataRow(s"SELECT task_id, task_time, record_time FROM qross_tasks_living WHERE job_id=$jobId AND status='${TaskStatus.READY}' LIMIT 1")
            }
            else {
                new DataRow()
            }
        }

        dh.close()

        if (continue) {
            //return
            Task(taskId, TaskStatus.EXECUTING).of(jobId).at(taskTime, recordTime)
        }
        else {
            //record and clear logger
            if (status != TaskStatus.EXECUTING && status != TaskStatus.EMPTY) {
                TaskRecorder.of(jobId, taskId, recordTime).debug(s"Task $taskId of job $jobId <$recordTime> finished with status ${status.toUpperCase()}.").dispose()
            }

            if (jobType != JobType.ENDLESS) {
                if (nextTask.isEmpty) {
                    Task(0)
                }
                else {
                    //return nothing
                    Task(nextTask.getLong("task_id"), TaskStatus.READY).of(jobId).at(nextTask.getString("task_time"), nextTask.getString("record_time"))
                }
            }
            else {
                //continue execute next task if job type is endless
                Task(-1, status).of(jobId)
            }
        }
    }

    def getTaskLogs(jobId: Int, taskId: Long, recordTime: String, cursor: Int, actionId: Long, mode: String): String = {

        val datetime = new io.qross.time.DateTime(recordTime)
        val path = s"""${Global.QROSS_HOME}/tasks/${datetime.getString("yyyyMMdd")}/$jobId/${taskId}_${datetime.getString("HHmmss")}.log"""
        val file = new File(path)
        if (file.exists()) {
            val where = {
                if (mode == "debug") {
                    if (actionId > 0) {
                        s"WHERE actionId=$actionId AND logType<>'INFO'"
                    }
                    else {
                        "WHERE logType<>'INFO'"
                    }
                }
                else if (mode == "error") {
                    if (actionId > 0) {
                        s"WHERE actionId=$actionId AND logType='ERROR'"
                    }
                    else {
                        "WHERE logType='ERROR'"
                    }
                }
                else if (actionId > 0) {
                    "WHERE actionId=" + actionId
                }
                else {
                    ""
                }
            }

            val dh = DataHub.QROSS
            dh.openJsonFile(path).asTable("logs")
            val result = s"""{"logs": ${dh.executeDataTable(s"SELECT * FROM :logs SEEK $cursor $where LIMIT 100").toString}, "cursor": ${dh.cursor} }"""
            dh.close()

            result
        }
        else {
            val dh = DataHub.QROSS

            val status = dh.executeSingleValue(s"SELECT status FROM qross_tasks WHERE id=$taskId AND record_time='$recordTime'")
                .orElse(dh.executeSingleValue(s"SELECT status FROM qross_tasks_records WHERE task_id=$taskId AND record_time='$recordTime'")).asText("")

            val result = {
                if (Set[String](TaskStatus.NEW, TaskStatus.INITIALIZED, TaskStatus.READY, TaskStatus.EXECUTING).contains(status)) {
                    val where = {
                        if (mode == "debug") {
                            if (actionId > 0) {
                                s"AND action_id=$actionId AND log_type<>'INFO'"
                            }
                            else {
                                "AND log_type<>'INFO'"
                            }
                        }
                        else if (mode == "error") {
                            if (actionId > 0) {
                                s"AND action_id=$actionId AND log_type='ERROR'"
                            }
                            else {
                                "AND log_type='ERROR'"
                            }
                        }
                        else if (actionId > 0) {
                            s"AND action_id=$actionId"
                        }
                        else {
                            ""
                        }
                    }

                    val table = dh.executeDataTable(s"SELECT id, command_id AS commandId, action_id AS actionId, log_type AS logType, log_text AS logText FROM qross_tasks_logs WHERE ${ if(cursor != 0) s"id>${Math.abs(cursor)} AND" else "" } task_id=$taskId AND record_time='$recordTime' $where LIMIT 100")
                    if (table.nonEmpty) {
                        s"""{"logs": ${table.toString}, "cursor": -${table.lastRow.head.getLong("id")} }"""
                    }
                    else {
                        s"""{"logs": [], "cursor": $cursor}"""
                    }
                }
                else {
                    """{"logs": [], "cursor": 0, "error": "File not found."}"""
                }
            }

            dh.close()

            result
        }
    }
}