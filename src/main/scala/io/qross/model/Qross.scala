package io.qross.model

import java.io.File

import io.qross.core.{DataHub, DataRow}
import io.qross.ext.Output
import io.qross.ext.Output._
import io.qross.ext.TypeExt._
import io.qross.fs.{Directory, ResourceFile}
import io.qross.jdbc.DataSource
import io.qross.keeper.{Keeper, Setting}
import io.qross.model.QrossTask._
import io.qross.net.Http
import io.qross.pql.PQL
import io.qross.script.Script
import io.qross.setting.{Configurations, Environment, Global}
import io.qross.time.{ChronExp, DateTime, Timer}
import io.qross.fs.TextFile._

import scala.collection.mutable

object Qross {

    implicit class HashMap$Nodes(nodes: mutable.HashMap[String, Long]) {
        def freest(): String = {
            val node =  nodes.reduce((n1, n2) => if (n2._2 > n1._2) n2 else n1)._1
            nodes += node -> (nodes(node) - 1)
            node
        }
    }
    
    def start(): Unit = {
        val ds = DataSource.QROSS
        val address = Keeper.NODE_ADDRESS

        ds.executeNonQuery(s"INSERT INTO qross_keeper_nodes (node_address, executors) VALUES ('${Keeper.NODE_ADDRESS}', ${Workshop.MAX}) ON DUPLICATE KEY UPDATE status='online', online_time=NOW(), disconnection=0, disconnect_time=NULL, offline_time=NULL, executors=${Workshop.MAX}")

        if (!ds.executeExists(s"SELECT id FROM qross_keeper_beats WHERE node_address='$address' AND actor_name='Keeper'")) {
            ds.executeNonQuery(s"INSERT INTO qross_keeper_beats (node_address, actor_name) VALUES ('$address', 'Keeper'), ('$address', 'TaskProducer'), ('$address', 'TaskStarter'), ('$address', 'TaskChecker'), ('$address', 'TaskExecutor'), ('$address', 'TaskLogger'), ('$address', 'NoteProcessor'), ('$address', 'NoteQuerier'), ('$address', 'Repeater'), ('$address', 'Inspector')");
        }

        //clear locks
        ds.executeNonQuery(s"UPDATE qross_keeper_locks SET node_address='', tick='' WHERE node_address='$address'")

        val method = {
            if (ds.executeExists(s"SELECT id FROM qross_keeper_beats WHERE node_address='$address' AND actor_name='Keeper' AND status='rest'")) {
                "manual"
            }
            else {
                "crontab"
            }
        }

        ds.executeNonQuery(s"INSERT INTO qross_keeper_running_records (node_address, method) VALUES ('$address', '$method')")
        ds.executeNonQuery(s"UPDATE qross_keeper_beats SET status='running', start_time=NOW() WHERE node_address='$address' AND actor_name='Keeper'")

         ds.executeDataTable(s"SELECT event_function, event_value, event_option, '$address' AS node_address, '$method' AS method FROM qross_keeper_events WHERE event_name='onNodeStart'")
            .foreach(row => {
                row.getString("event_function") match {
                    case "SEND_MAIL" => sendMail("beats", s"Keeper Start by $method", row)
                    case "REQUEST_API" => requestApi(row)
                    case "EXECUTE_SCRIPT" => executeScript(row)
                    case _ =>
                }
            })

        ds.close()
    }

    // all available nodes
    def availableNodes: mutable.HashMap[String, Long] = {
        new mutable.HashMap[String, Long]() ++= DataSource.QROSS.queryDataMap[String, Long]("SELECT node_address, executors - busy_executors AS free_executors FROM qross_keeper_nodes WHERE status='online' AND disconnection=0")
    }

    def otherNodes: List[String] = {
        DataSource.QROSS.querySingleList[String](s"SELECT node_address FROM qross_keeper_nodes WHERE node_address<>'${Keeper.NODE_ADDRESS}' AND status='online' AND disconnection=0")
    }

    def idleNode: String = {
        DataSource.QROSS.querySingleValue("SELECT node_address FROM qross_keeper_nodes WHERE status='online' AND disconnection=0 ORDER BY busy_score ASC LIMIT 1").asText("")
    }

    def distribute(method: String, path: String): Unit = {
        otherNodes.foreach(address => {
            try {
                new Http(method, s"http://$address/${path}terminus=yes&token=${Global.KEEPER_HTTP_TOKEN}").request()
            }
            catch {
                case e: java.net.ConnectException => Qross.disconnect(address, e)
                case e: Exception => e.printStackTrace() // e.printReferMessage()
            }
        })
    }

    def disconnect(node: String, e: java.net.ConnectException): Unit = {

        Output.writeException(e.getReferMessage + s" to $node")

        val ds = DataSource.QROSS
        ds.executeNonQuery(s"UPDATE qross_keeper_nodes SET disconnection=disconnection+1 WHERE node_address='$node'")
        val count = ds.executeNonQuery(s"UPDATE qross_keeper_nodes SET disconnection=0, status='disconnected', disconnect_time=NOW() WHERE node_address='$node' AND disconnection>=3")

        if (count > 0) {
            //events
            ds.executeDataTable(s"SELECT event_function, event_value, event_option, '${Keeper.NODE_ADDRESS}' AS node_address, '$node' AS disconnected FROM qross_keeper_events WHERE event_name='onNodeDisconnect'")
                .foreach(row => {
                    row.getString("event_function") match {
                        case "SEND_MAIL" => sendMail("disconnection", s"Keeper Node is disconnected: $node", row)
                        case "REQUEST_API" => requestApi(row)
                        case "EXECUTE_SCRIPT" => executeScript(row)
                        case _ =>
                    }
                })
        }

        ds.close()
    }

    def shutting(): Unit = {
        val ds = DataSource.QROSS
        ds.executeNonQuery(s"UPDATE qross_keeper_beats SET status='rest', quit_time=NOW() WHERE node_address='${Keeper.NODE_ADDRESS}' AND actor_name='Keeper'")
        ds.executeNonQuery(s"UPDATE qross_keeper_nodes SET status='shutting' WHERE node_address='${Keeper.NODE_ADDRESS}'")
        ds.close()
    }

    def shutdown(): Unit = {
        val ds = DataSource.QROSS
        ds.executeNonQuery(s"UPDATE qross_keeper_nodes SET status='offline', offline_time=NOW() WHERE node_address='${Keeper.NODE_ADDRESS}'")
        ds.executeDataTable(s"SELECT event_function, event_value, event_option, '${Keeper.NODE_ADDRESS}' AS node_address FROM qross_keeper_events WHERE event_name='onNodeShutdown'")
            .foreach(row => {
                row.getString("event_function") match {
                    case "SEND_MAIL" => sendMail("shutdown", s"Keeper Node Shutdown: ${Keeper.NODE_ADDRESS}", row)
                    case "REQUEST_API" => requestApi(row)
                    case "EXECUTE_SCRIPT" => executeScript(row)
                    case _ =>
                }
            })

        ds.close()
    }
    
    def run(actor: String, message: String = ""): Unit = {
        writeLineWithSeal("SYSTEM",  s"<${Keeper.NODE_ADDRESS}> $actor start! " + message)
        DataSource.QROSS.queryUpdate(s"UPDATE qross_keeper_beats SET status='running', start_time=NOW() WHERE node_address='${Keeper.NODE_ADDRESS}' AND actor_name='$actor'")
    }

    def check(tick: String): Unit = {

        val dh = DataHub.QROSS
        val address = Keeper.NODE_ADDRESS

        writeLineWithSeal("SYSTEM", s"<$address> Inspector beat!")
        dh.set(s"UPDATE qross_keeper_beats SET last_beat_time=NOW() WHERE node_address='$address' AND actor_name='Inspector'")

        //node monitor
        dh.get(s"""SELECT COUNT(0) AS tasks FROM qross_tasks_living WHERE status='${TaskStatus.EXECUTING}' AND node_address='$address'""".stripMargin)
            .put(s"""INSERT IGNORE INTO qross_keeper_nodes_monitor (node_address, moment, cpu_usage, memory_usage, executing_tasks, busy_executors, busy_score)
               | VALUES ('$address', '$tick', ${Environment.cpuUsage}, ${Environment.systemMemoryUsage}, #tasks, ${Workshop.busy}, ${Workshop.busyScore})""".stripMargin)

        dh.get(s"SELECT cpu_usage, memory_usage, busy_executors, busy_score FROM qross_keeper_nodes_monitor WHERE node_address='$address' AND moment='$tick' LIMIT 1")
            .put(s"UPDATE qross_keeper_nodes SET cpu_usage=#cpu_usage, memory_usage=#memory_usage, busy_executors=#busy_executors, busy_score=#busy_score, status='online', disconnect_time=NULL, disconnection=0, offline_time=NULL WHERE node_address='$address'")

        //connect to other nodes
        dh.get(s"SELECT node_address FROM qross_keeper_nodes WHERE node_address<>'$address' AND status='online'")
            .takeOut()
            .foreach(row => {
                    val node = row.getString("node_address")
                    try {
                        Http.GET(s"http://$node").request()
                    }
                    catch {
                        case e: java.net.ConnectException =>
                            Output.writeException(s"${e.getReferMessage} to $node")
                            dh.executeNonQuery(s"UPDATE qross_keeper_nodes SET disconnection=disconnection+1 WHERE node_address='$node'")
                            dh.executeNonQuery(s"UPDATE qross_keeper_nodes SET disconnection=0, status='disconnected', disconnect_time=NOW() WHERE node_address='$node' AND disconnection>=3")
                        case e: Exception => e.printStackTrace() //e.printReferMessage()
                    }
                })

        //slow tasks - only check
        dh.get(s"""SELECT A.job_id, A.owner, A.title, B.event_name, B.event_value, B.event_function, B.event_limit, B.event_option, C.task_id, C.status, C.start_mode, C.task_time, C.record_time FROM
                                (SELECT id AS job_id, title, owner, executing_overtime FROM qross_jobs WHERE executing_overtime>0) A
                           INNER JOIN (SELECT job_id, event_name, event_function, event_limit, event_value, event_option FROM qross_jobs_events WHERE event_name='onTaskSlow' AND enabled='yes') B ON A.job_id=B.job_id
                           INNER JOIN (
                             SELECT S.task_id, S.job_id, S.task_time, S.record_time, S.status, S.start_mode, T.event_name FROM qross_tasks_living S LEFT JOIN qross_tasks_events T
                              ON S.task_id=T.task_id AND S.record_time=T.record_time AND T.event_name='onTaskSlow' WHERE T.id IS NOT NULL
                           ) C ON A.job_id=C.job_id AND TIMESTAMPDIFF(MINUTE, C.record_time, NOW())>A.executing_overtime;
                            """)
            .cache("slow_tasks_events")
        if (dh.nonEmpty) {
            writeLineWithSeal("SYSTEM", s"${dh.COUNT_OF_LAST_GET} event(s) have been fired by slow Tasks.")
            dh.openCache()
                .get("SELECT task_id, job_id, title, owner, task_time, record_time, event_value AS receivers, event_name, event_function, event_limit, '' AS event_result FROM slow_tasks_events WHERE event_function='SEND_MAIL_TO' AND INSTR(event_limit, start_mode)>0")
                    .sendEmail(TaskStatus.SLOW)
                .get("SELECT task_id, job_id, title, owner, task_time, record_time, event_value AS api, event_option AS method, event_name, event_function, event_limit, '' AS event_result FROM slow_tasks_events WHERE event_function='REQUEST_API' AND INSTR(event_limit, start_mode)>0")
                    .requestApi(TaskStatus.SLOW)
                .get("SELECT task_id, job_id, title, owner, task_time, record_time, event_value AS value, event_name, event_function, event_limit, '' AS event_result FROM slow_tasks_events WHERE INSTR(event_function, 'CUSTOM_')>0 AND INSTR(event_limit, start_mode)>0")
                    .fireCustomEvent(TaskStatus.SLOW)
                .get("SELECT task_id, job_id, title, owner, task_time, record_time, B.event_option AS script_type, event_value AS script, event_name, event_function, event_limit, '' AS event_result FROM slow_tasks_events WHERE event_function='EXECUTE_SCRIPT' AND INSTR(event_limit, start_mode)>0")
                    .runScript(TaskStatus.SLOW)
        }

        dh.clear()
            .openQross()
            .get("SELECT GROUP_CONCAT(actor_name) AS actor_names FROM qross_keeper_beats WHERE actor_name IN ('Keeper', 'TaskProducer', 'TaskStarter', 'TaskChecker', 'TaskExecutor', 'TaskLogger', 'NoteProcessor', 'NoteQuerier', 'Repeater') AND status='rest'")

        if (dh.nonEmpty) {
            val restActors = dh.firstCellStringValue
            dh.clear()
                .get(s"SELECT event_function, event_value, event_option, '$tick' AS tick, '$address' AS node_address, '$restActors' as actors FROM qross_keeper_events WHERE event_name='onNodeBeatException'")
                .takeOut()
                .foreach(row => {
                    row.getString("event_function") match {
                        case "SEND_MAIL" => sendMail("beats", s"Keeper Beats Exception: $restActors at $tick", row)
                        case "REQUEST_API" => requestApi(row)
                        case "EXECUTE_SCRIPT" => executeScript(row)
                        case "RESTART_KEEPER" => Configurations.set("QUIT_ON_NEXT_BEAT", true)
                        case _ =>
                    }
                })
        }
        else if (new DateTime(tick).matches(Setting.BEAT_EVENTS_FIRE_FREQUENCY)) {
            dh.clear()
                .get(s"SELECT event_function, event_value, event_option, '$tick' AS tick, '$address' AS node_address FROM qross_keeper_events WHERE event_name='onNodeBeat'")
                .takeOut()
                .foreach(row => {
                    row.getString("event_function") match {
                        case "SEND_MAIL" => sendMail("beats", s"Keeper Beats regularly at $tick", row)
                        case "REQUEST_API" => requestApi(row)
                        case "EXECUTE_SCRIPT" => executeScript(row)
                        case _ =>
                    }
                })
        }

        Workshop.delay()

        dh.clear()
            .openQross()
            //close job if expired
            .get(s"SELECT id FROM qross_jobs WHERE enabled='yes' AND closing_time<>'' AND TIMESTAMPDIFF(MINUTE, '$tick', closing_time)<=1")
                .put("UPDATE qross_jobs SET enabled='no', next_tick='', closing_time='' WHERE id=#id")
                .foreach(row => {
                    writeDebugging("Job " + row.getString("id") + " has been closed.")
                })
            //open job if arrived
            .get(s"SELECT id, opening_time, cron_exp, next_tick FROM qross_jobs WHERE enabled='no' AND opening_time<>'' AND TIMESTAMPDIFF(MINUTE, '$tick', opening_time)<=1")
                .foreach(row => {
                    row.set("next_tick", ChronExp(row.getString("cron_exp")).getNextTickOrNone(row.getDateTime("opening_time")))
                    writeDebugging("Job " + row.getString("id") + " is openning.")
                })
                .put("UPDATE qross_jobs SET enabled='yes', next_tick='#next_tick', opening_time='' WHERE id=#id")
                .clear()


        val moment = new DateTime(tick)

        if (moment.matches("0 0 * * * ? *")) {
            dh.set(s"INSERT IGNORE INTO qross_tasks_stats (node_address, moment) VALUES ('$address', '${tick.dropRight(5) + "00:00"}')")
        }

        //clear tasks
        if (moment.matches("0 37 * * * ? *")) {

            writeMessage("Cleaning tasks mechanism is ready to execute.")

            val locked = dh.executeNonQuery(s"UPDATE qross_keeper_locks SET node_address='$address', tick='$tick', lock_time=NOW() WHERE lock_name='CLEAN-TASKS' AND tick<>'$tick'") == 1
            if (locked) {
                dh.get(
                    s"""SELECT B.job_id, A.keep_x_task_records FROM qross_jobs A
                INNER JOIN (SELECT job_id, COUNT(0) AS task_amount FROM qross_tasks GROUP BY job_id) B ON A.id=B.job_id
            WHERE A.keep_x_task_records>0 AND B.task_amount>A.keep_x_task_records""".stripMargin)
                    .pass(s"SELECT id AS task_id, job_id FROM qross_tasks WHERE job_id=#job_id AND status NOT IN ('${TaskStatus.NEW}', '${TaskStatus.INITIALIZED}', '${TaskStatus.READY}', '${TaskStatus.EXECUTING}', '${TaskStatus.FINISHED}') ORDER BY id DESC LIMIT #keep_x_task_records, 1", "job_id" -> 0, "keep_x_task_records" -> 0)
                        .put("UPDATE qross_tasks SET status='to_be_deleted' WHERE job_id=#job_id AND id<#task_id")
                        .put("UPDATE qross_tasks_records SET status='to_be_deleted' WHERE job_id=#job_id AND task_id<#task_id")
                        .clear()
            }

            dh.executeDataTable(s"""SELECT id AS task_id, job_id, record_time FROM qross_tasks WHERE status='to_be_deleted' AND node_address='$address' UNION SELECT task_id, job_id, record_time FROM qross_tasks_records WHERE status='to_be_deleted' AND node_address='$address'""")
                .foreach(task => {
                    new File(s"""${Global.QROSS_HOME}/tasks/${task.getInt("job_id")}/${ task.getDateTime("record_time").format("yyyyMMdd") }/${task.getLong("task_id")}_${ task.getDateTime("record_time").format( "HHmmss") }.log""").delete()
                })

            dh.get(s"SELECT id AS task_id FROM qross_tasks WHERE status='to_be_deleted' AND node_address='$address'")
                .put("DELETE FROM qross_tasks_dependencies WHERE task_id=#task_id")
                .put("DELETE FROM qross_tasks_dags WHERE task_id=#task_id")
                .put("DELETE FROM qross_tasks_events WHERE task_id=#task_id")
                .put("DELETE FROM qross_tasks_records WHERE task_id=#task_id")
                .put("DELETE FROM qross_tasks_abnormal WHERE task_id=#task_id")
                .put("DELETE FROM qross_tasks_to_be_start WHERE task_id=#task_id")
                .put("DELETE FROM qross_tasks WHERE id=#task_id")

            writeMessage("Tasks cleaning has finished.")
        }

        // 03:15 every day - clear keeper logs
        if (moment.matches("0 15 3 * * ? *")) {

            val ago = moment.minusDays(Setting.KEEP_LOGS_FOR_X_DAYS)
            val point = ago.format("yyyy-MM-dd 00:00:00")
            val day = ago.format("yyyyMMdd")

            val locked = dh.executeNonQuery(s"UPDATE qross_keeper_locks SET node_address='$address', tick='$tick', lock_time=NOW() WHERE lock_name='CLEAN-LOGS' AND tick<>'$tick'") == 1
            if (locked) {
                dh.executeNonQuery(s"DELETE FROM qross_stuck_records WHERE create_time<'$point'")
                dh.executeNonQuery(s"DELETE FROM qross_keeper_exceptions WHERE create_time<'$point'")
                dh.executeNonQuery(s"DELETE FROM qross_keeper_nodes_monitor WHERE moment<'$point'")
            }

            Directory.listDirs(s"""${Global.QROSS_HOME}/keeper/logs/""").foreach(dir => {
                if (dir.getName.toInt < day) {
                    Directory.delete(dir.getAbsolutePath)
                }
            })

            writeMessage("System records and logs have been cleaned.")
        }

        dh.close()
    }

    def beat(actor: String): Unit = {
        writeLineWithSeal("SYSTEM", s"<${Keeper.NODE_ADDRESS}> $actor beat!")
        DataSource.QROSS.queryUpdate(s"UPDATE qross_keeper_beats SET last_beat_time=NOW() WHERE node_address='${Keeper.NODE_ADDRESS}' AND actor_name='$actor'")
    }

    def quit(actor: String): Unit = {
        writeLineWithSeal("SYSTEM", s"<${Keeper.NODE_ADDRESS}> $actor quit!")
        DataSource.QROSS.queryUpdate(s"UPDATE qross_keeper_beats SET status='rest', quit_time=NOW() WHERE node_address='${Keeper.NODE_ADDRESS}' AND actor_name='$actor'")

    }

    def waitAndStop(): Unit = {
        val dh = DataHub.QROSS
        val address = Keeper.NODE_ADDRESS
        dh.get(s"SELECT id FROM qross_keeper_running_records WHERE node_address='$address' ORDER BY id DESC LIMIT 1")
            .put("UPDATE qross_keeper_running_records SET stop_time=NOW(), duration=TIMESTAMPDIFF(SECOND, start_time, NOW()) WHERE id=#id")

        while(dh.get(s"SELECT actor_name FROM qross_keeper_beats WHERE node_address='$address' AND status='running' LIMIT 1").nonEmpty && !Setting.FORCE_TO_SHUTDOWN) {
            writeLineWithSeal("SYSTEM", dh.firstCellStringValue + " is still working.")
            dh.clear()
            val amount = dh.get(s"SELECT COUNT(0) AS amount FROM qross_tasks_living WHERE status='${TaskStatus.EXECUTING}' AND node_address='$address'").firstCellIntValue
            if (amount > 1) {
                writeLineWithSeal("SYSTEM", s"There are $amount tasks still to be done.")
            }
            else if (amount == 1) {
                writeLineWithSeal("SYSTEM", "There is 1 task still to be done.")
            }
            dh.clear()

            //TaskLogger 退出后记录日志的工作转到这里
            TaskRecorder.save()

            Timer.sleep(2000)
        }
        dh.close()
    }

    def getKeeperLogs(hour: String, cursor: Int): String = {
        val limit = 200
        val current = DateTime.now.getString("yyyyMMdd/HH")
        val path = s"""${Global.QROSS_HOME}keeper/logs/$hour.log"""
        val file = new File(path)
        if (file.exists()) {
            val dh = DataHub.QROSS
            dh.openTextFile(file).delimitedBy("""[\[\]]""").withColumns("datetime TEXT", "log_type TEXT", "log_text TEXT").asTable("logs")
            val logs = dh.executeDataTable(s"SELECT * FROM :logs SEEK $cursor LIMIT $limit")
            if (logs.size < limit && hour != current) {
                val nextPath = s"""${Global.QROSS_HOME}keeper/logs/$current.log"""
                val nextFile = new File(nextPath)
                if (nextFile.exists()) {
                    dh.openTextFile(nextFile).delimitedBy("""[\[\]]""").withColumns("datetime TEXT", "log_type TEXT", "log_text TEXT").asTable("next_logs")
                    logs.merge(dh.executeDataTable(s"SELECT * FROM :next_logs SEEK 0 LIMIT ${limit - logs.size}"))
                }
            }
            val result = s"""{"logs": ${logs.toString}, "cursor": ${dh.cursor}, "hour": "$current" }"""
            dh.close()

            result
        }
        else {
            """{"logs": [], "cursor": -1, "error": "File not found."}"""
        }
    }

    def sendMail(template: String, title: String, args: DataRow): Unit = {
        args.remove("event_value")
        args.remove("event_option")
        if (Global.EMAIL_NOTIFICATION) {
            try {
                ResourceFile.open(s"/templates/$template.html")
                    .replaceWith(args)
                    .writeEmail(title)
                    .to(DataSource.QROSS.executeSingleValue("SELECT GROUP_CONCAT(CONCAT(fullname, '<', email, '>')) AS keeper FROM qross_users WHERE role IN ('master', 'keeper') AND enabled='yes'").asText(""))
                    .send()

                writeDebugging(s"Keeper Event: Mail '$title' was sent.")
            }
            catch {
                case e: Exception => e.printStackTrace() //e.printReferMessage()
            }
        }
    }

    def requestApi(args: DataRow): Unit = {
        val method = args.getString("event_option", "GET")
        val url = args.getString("event_value")
        args.remove("event_value")
        args.remove("event_option")
        val api = PQL.openEmbedded(url).place(args).run().toString.replace(" ", "%20").replace("&amp;", "&")
        if (api != "") {
            try {
                val result = new Http(method, api).request()
                writeDebugging(s"Keeper Event: request url '$api' and result is $result.")
            }
            catch {
                case e: Exception => e.printStackTrace() //e.printReferMessage()
            }
        }
    }

    def executeScript(args: DataRow): Unit = {
        val scriptType = args.getString("event_option")
        val scriptLogic = args.getString("event_value")
        args.remove("event_value")
        args.remove("event_option")

        try {
            val result = {
                scriptType.toLowerCase() match {
                    case "pql" => new PQL(scriptLogic, DataHub.DEFAULT).place(args).run()
                    //shell和python支持PQL的嵌入表达式
                    case "shell" => Script.runShell(PQL.openEmbedded(scriptLogic).place(args).run().asInstanceOf[String])
                    case "python" => Script.runPython(PQL.openEmbedded(scriptLogic).place(args).run().asInstanceOf[String])
                    case _ => ""
                }
            }

            writeDebugging(s"Keeper Event: run script and the result is $result.")
        }
        catch {
            case e: Exception => e.printStackTrace() //e.printReferMessage()
        }
    }
}
