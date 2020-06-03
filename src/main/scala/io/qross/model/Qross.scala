package io.qross.model

import io.qross.core.DataHub
import io.qross.ext.Output._
import io.qross.fs.{Directory, ResourceFile}
import io.qross.jdbc.DataSource
import io.qross.keeper.Setting
import io.qross.net.Email
import io.qross.setting.{Configurations, Global}
import io.qross.time.{ChronExp, DateTime, Timer}

object Qross {
    
    def start(): Unit = {
        val ds = DataSource.QROSS
        val method =
            if (ds.executeExists("SELECT id FROM qross_keeper_beats WHERE actor_name='Keeper' AND status='rest'")) {
                "manual"
            }
            else {
                "crontab"
            }
        
        ds.executeNonQuery(s"INSERT INTO qross_keeper_running_records (method) VALUES ('$method')")
        
        if (method != "manual" && Setting.MASTER_USER_GROUP != "") {
            Email.write(s"RESTART: Keeper start by <$method> at " + DateTime.now.getString("yyyy-MM-dd HH:mm:ss")).to(Setting.MASTER_USER_GROUP).send()
        }
    
        ds.executeNonQuery("UPDATE qross_keeper_beats SET status='running',start_time=NOW() WHERE actor_name='Keeper'")
        
        ds.close()
    }
    
    def run(actor: String, message: String = ""): Unit = {
        writeDebugging(actor + " start! " + message)
        DataSource.QROSS.queryUpdate(s"UPDATE qross_keeper_beats SET status='running',start_time=NOW() WHERE actor_name='$actor'")
    }
    
    def beat(actor: String): Unit = {
        writeMessage(actor + " beat!")
        DataSource.QROSS.queryUpdate(s"UPDATE qross_keeper_beats SET last_beat_time=NOW() WHERE actor_name='$actor'")
    }
    
    def quit(actor: String): Unit = {
        writeDebugging(s"$actor quit!")
        DataSource.QROSS.queryUpdate(s"UPDATE qross_keeper_beats SET status='rest', quit_time=NOW() WHERE actor_name='$actor'")
        
    }
    
    def waitAndStop(): Unit = {
        val dh = DataHub.QROSS
        dh.get("SELECT id FROM qross_keeper_running_records ORDER BY id DESC LIMIT 1")
            .put("UPDATE qross_keeper_running_records SET status='stopping', stop_time=NOW(), duration=TIMESTAMPDIFF(SECOND, start_time, NOW()) WHERE id=#id")

        while(dh.get("SELECT actor_name FROM qross_keeper_beats WHERE status='running' LIMIT 1").nonEmpty) {
            writeDebugging(dh.firstRow.getString("actor_name") + " still working.")
            dh.clear()
            dh.get("SELECT COUNT(0) AS amount FROM qross_tasks WHERE status='executing'")
            writeDebugging("There is " + dh.firstRow.getString("amount") + " tasks still executing.")
            dh.clear()
            Timer.sleep(5000)
        }

        dh.close()
    }
    
    def checkBeatsAndRecords(): Unit = {

        //var now = DateTime.now
        var now = DateTime.now
        val tick = now.getString("yyyy-MM-dd HH:mm:ss")
        now = now.setSecond(0)

        writeDebugging(s"Inspector beats at $now")

        val dh = DataHub.QROSS
        if (dh.executeSingleValue("SELECT status FROM qross_keeper_running_records ORDER BY id DESC LIMIT 1").asText == "running") {

            var error = ""
            var toSend = true
            var title = ""

            //irregular - will restart Keeper
            dh.get("SELECT actor_name FROM qross_keeper_beats WHERE actor_name IN ('Keeper', 'Messenger', 'TaskProducer', 'TaskStarter', 'TaskChecker', 'TaskExecutor', 'TaskLogger') AND status='rest'")
            if (dh.nonEmpty) {
                //quit system and auto restart
                Configurations.set("QUIT_ON_NEXT_BEAT", true)
                //mail info
                error = "Keeper will restart on next tick. Please wait util all executing task finished."
                title = s"Keeper Beats Exception: ${dh.getColumn("actor_name").mkString(",")} at $tick"

                writeWarning(error)
            }
            else if (now.matches(Setting.BEATS_MAILING_FREQUENCY)) {
                //regular
                title = s"Keeper Beats regularly at $tick"
            }
            else {
                toSend = false
            }

            //send beats mail
            if (toSend && (Setting.KEEPER_USER_GROUP != "" || Setting.MASTER_USER_GROUP != "")) {
                ResourceFile.open("/templates/beats.html")
                    .replace("#{tick}", tick)
                    .replace("#{error}", error)
                    .replace("#{beats}", dh.executeDataTable("SELECT CONCAT(actor_name, ' - ', status, ' - ', last_beat_time) AS info FROM qross_keeper_beats ORDER BY id DESC").getColumn("info").mkString("<br/>"))
                    .writeEmail(title)
                    .to(if (Setting.KEEPER_USER_GROUP != "") Setting.KEEPER_USER_GROUP else Setting.MASTER_USER_GROUP)
                    .cc(if (Setting.KEEPER_USER_GROUP != "") Setting.MASTER_USER_GROUP else "")
                    .send()

                writeDebugging("Beats mail has been sent.")
            }

            //slow tasks
            import io.qross.model.QrossTask._
            dh.get(s"""SELECT A.job_id, A.owner, A.title, B.event_value, B.event_function, B.event_limit, C.task_id, C.status, C.start_mode, C.task_time, C.record_time FROM
                    (SELECT id AS job_id, title, owner, warning_overtime FROM qross_jobs WHERE enabled='yes' AND warning_overtime>0) A
               INNER JOIN (SELECT job_id, event_function, event_limit, event_value, event_option FROM qross_jobs_events WHERE event_name='onTaskSlow' AND enabled='yes') B ON A.job_id=B.job_id
               INNER JOIN (SELECT id AS task_id, job_id, task_time, record_time, status, start_mode, create_time FROM qross_tasks WHERE status IN ('${TaskStatus.NEW}', '${TaskStatus.INITIALIZED}', '${TaskStatus.READY}', '${TaskStatus.EXECUTING}')) C
                ON A.job_id=C.job_id AND TIMESTAMPDIFF(MINUTE, B.create_time, NOW())>A.warning_overtime
                """)
                .cache("slow_tasks_events")
            dh.openCache()
                .get("SELECT task_id, job_id, title, owner, task_time, record_time, event_value AS receivers, event_name, event_function, event_limit, '' AS event_result FROM slow_tasks_events WHERE event_function='SEND_MAIL_TO' AND INSTR(event_limit, start_mode)>0")
                    .sendEmail(s"${TaskStatus.SLOW}")
                .get("SELECT task_id, job_id, title, owner, task_time, record_time, event_value AS api, event_name, event_function, event_limit, '' AS event_result FROM slow_tasks_events WHERE event_function='REQUEST_API' AND INSTR(B.event_limit, A.start_mode)>0")
                    .requestApi(s"${TaskStatus.SLOW}")
                .get("SELECT task_id, job_id, title, owner, task_time, record_time, event_value AS pql, event_name, event_function, event_limit, '' AS event_result FROM slow_tasks_events WHERE event_function='EXECUTE_PQL' AND INSTR(B.event_limit, A.start_mode)>0")
                    .runPQL(s"${TaskStatus.SLOW}")

            //close job if expired
            dh.clear().openQross()
                .get(s"SELECT id FROM qross_jobs WHERE enabled='yes' AND closing_time<>'' AND TIMESTAMPDIFF(MINUTE, '${now.getString("yyyy-MM-dd HH:mm:00")}', closing_time)<=1")
                    .put("UPDATE qross_jobs SET enabled='no', next_tick='N/A', closing_time='' WHERE id=#id")
                    .foreach(row => {
                        writeMessage("Job " + row.getString("id") + " has been closed.")
                    })
                //open job if arrived
                .get(s"SELECT id, opening_time, cron_exp, next_tick FROM qross_jobs WHERE enabled='no' AND opening_time<>'' AND TIMESTAMPDIFF(MINUTE, '${now.getString("yyyy-MM-dd HH:mm:00")}', opening_time)<=1")
                .foreach(row => {
                    row.set("next_tick", ChronExp(row.getString("cron_exp")).getNextTickOrNone(row.getDateTime("opening_time")))
                    writeMessage("Job " + row.getString("id") + " is opening.")
                })
                .put("UPDATE qross_jobs SET enabled='yes', next_tick='#next_tick', opening_time='' WHERE id=#id")
        }

        //disk space monitor
        if (now.matches(Setting.DISK_MONITOR_FREQUENCY)) {
            dh.set(s"INSERT INTO qross_space_monitor (moment, keeper_logs_space_usage, task_logs_space_usage, temp_space_usage) VALUES ('${now.getString("yyyy-MM-dd HH:mm:ss")}', ${Directory.spaceUsage(Global.QROSS_HOME + "logs/")}, ${Directory.spaceUsage(Global.QROSS_HOME + "tasks/")}, ${Directory.spaceUsage(Global.QROSS_HOME + "temp/")})")
            writeDebugging("Disk info has been saved.")
        }
        
        dh.close()
    }
}
