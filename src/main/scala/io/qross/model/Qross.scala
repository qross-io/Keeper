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
        
        if (method != "manual") {
            Email.write(s"RESTART: Keeper start by <$method> at " + DateTime.now.getString("yyyy-MM-dd HH:mm:ss"))
                .to(ds.executeSingleValue("SELECT GROUP_CONCAT(CONCAT(fullname, '<', email, '>')) AS keeper FROM qross_users WHERE role='keeper' AND enabled='yes'").asText(""))
                .cc(ds.executeSingleValue("SELECT GROUP_CONCAT(CONCAT(fullname, '<', email, '>')) AS master FROM qross_users WHERE role='master' AND enabled='yes'").asText(""))
                .send()
        }
    
        ds.executeNonQuery("UPDATE qross_keeper_beats SET status='running',start_time=NOW() WHERE actor_name='Keeper'")
        
        ds.close()
    }
    
    def run(actor: String, message: String = ""): Unit = {
        writeLineWithSeal("SYSTEM", actor + " start! " + message)
        DataSource.QROSS.queryUpdate(s"UPDATE qross_keeper_beats SET status='running',start_time=NOW() WHERE actor_name='$actor'")
    }
    
    def beat(actor: String): Unit = {
        writeLineWithSeal("SYSTEM", actor + " beat!")
        DataSource.QROSS.queryUpdate(s"UPDATE qross_keeper_beats SET last_beat_time=NOW() WHERE actor_name='$actor'")
    }
    
    def quit(actor: String): Unit = {
        writeLineWithSeal("SYSTEM", s"$actor quit!")
        DataSource.QROSS.queryUpdate(s"UPDATE qross_keeper_beats SET status='rest', quit_time=NOW() WHERE actor_name='$actor'")
        
    }
    
    def waitAndStop(): Unit = {
        val dh = DataHub.QROSS
        dh.get("SELECT id FROM qross_keeper_running_records ORDER BY id DESC LIMIT 1")
            .put("UPDATE qross_keeper_running_records SET status='stopping', stop_time=NOW(), duration=TIMESTAMPDIFF(SECOND, start_time, NOW()) WHERE id=#id")

        while(dh.get("SELECT actor_name FROM qross_keeper_beats WHERE status='running' LIMIT 1").nonEmpty) {
            writeLineWithSeal("SYSTEM", dh.firstRow.getString("actor_name") + " is still working.")
            dh.clear()
            dh.get("SELECT COUNT(0) AS amount FROM qross_tasks WHERE status='executing'")
            writeLineWithSeal("SYSTEM", "There is " + dh.firstRow.getString("amount") + " tasks still to be done.")
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

        writeLineWithSeal("SYSTEM", s"Inspector beats at $now")

        val dh = DataHub.QROSS
        if (dh.executeSingleValue("SELECT status FROM qross_keeper_running_records ORDER BY id DESC LIMIT 1").asText == "running") {

            var toSend = true
            var title = ""

            //irregular - will restart Keeper
            dh.get("SELECT actor_name FROM qross_keeper_beats WHERE actor_name IN ('Keeper', 'Messenger', 'TaskProducer', 'TaskStarter', 'TaskChecker', 'TaskExecutor', 'TaskLogger', 'NoteProcessor', 'NoteQuerier') AND status='rest'")
            if (dh.nonEmpty) {
                //quit system and auto restart
                //这样重启无效，因为与Keeper是两个进程，不能通信，而且有可能在重启过程中，所以暂不实现自动重启逻辑
                //Configurations.set("QUIT_ON_NEXT_BEAT", true)

                //mail info
                title = s"Keeper Beats Exception: ${dh.getColumn("actor_name").mkString(",")} at $tick"
            }
            else if (now.matches(Setting.BEATS_MAILING_FREQUENCY)) {
                //regular
                title = s"Keeper Beats regularly at $tick"
            }
            else {
                toSend = false
            }
            dh.clear()

            //send beats mail
            if (toSend) {
                ResourceFile.open("/templates/beats.html")
                    .replace("#{tick}", tick)
                    .writeEmail(title)
                    .to(dh.executeSingleValue("SELECT GROUP_CONCAT(CONCAT(fullname, '<', email, '>')) AS keeper FROM qross_users WHERE role='keeper' AND enabled='yes'").asText(""))
                    .cc(dh.executeSingleValue("SELECT GROUP_CONCAT(CONCAT(fullname, '<', email, '>')) AS master FROM qross_users WHERE role='master' AND enabled='yes'").asText(""))
                    .send()

                writeLineWithSeal("SYSTEM", "Beats mail has been sent.")
            }

            //slow tasks
            import io.qross.model.QrossTask._
            dh.get(s"""SELECT A.job_id, A.owner, A.title, B.event_name, B.event_value, B.event_function, B.event_limit, B.event_option, C.task_id, C.status, C.start_mode, C.task_time, C.record_time FROM
                                (SELECT id AS job_id, title, owner, warning_overtime FROM qross_jobs WHERE warning_overtime>0) A
                           INNER JOIN (SELECT job_id, event_name, event_function, event_limit, event_value, event_option FROM qross_jobs_events WHERE event_name='onTaskSlow' AND enabled='yes') B ON A.job_id=B.job_id
                           INNER JOIN (
                             SELECT S.id AS task_id, S.job_id, S.task_time, S.record_time, S.status, S.start_mode, S.create_time, T.event_name FROM qross_tasks S LEFT JOIN qross_tasks_events T
                              ON S.id=T.task_id AND S.record_time=T.record_time AND T.event_name='onTaskSlow' WHERE S.status IN ('new', 'initialized', 'ready', 'executing') AND T.id IS NULL
                           ) C ON A.job_id=C.job_id AND TIMESTAMPDIFF(MINUTE, C.record_time, NOW())>A.warning_overtime;
                            """)
                .cache("slow_tasks_events")

            if (dh.nonEmpty) {
                writeLineWithSeal("SYSTEM", s"${dh.COUNT_OF_LAST_GET} event(s) have been fired by slow Tasks.")

                dh.openCache()
                    .get("SELECT task_id, job_id, title, owner, task_time, record_time, event_value AS receivers, event_name, event_function, event_limit, '' AS event_result FROM slow_tasks_events WHERE event_function='SEND_MAIL_TO' AND INSTR(event_limit, start_mode)>0")
                        .sendEmail(TaskStatus.SLOW)
                    .get("SELECT task_id, job_id, title, owner, task_time, record_time, event_value AS api, event_option AS method, event_name, event_function, event_limit, '' AS event_result FROM slow_tasks_events WHERE event_function='REQUEST_API' AND INSTR(event_limit, start_mode)>0")
                        .requestApi(TaskStatus.SLOW)
                    .get("SELECT task_id, job_id, title, owner, task_time, record_time, event_value AS roles, event_name, event_function, event_limit, '' AS event_result FROM slow_tasks_events WHERE INSTR(event_function, 'CUSTOM_')>0 AND INSTR(event_limit, start_mode)>0")
                        .fireCustomEvent(TaskStatus.SLOW)
                    .get("SELECT task_id, job_id, title, owner, task_time, record_time, event_value AS pql, event_name, event_function, event_limit, '' AS event_result FROM slow_tasks_events WHERE event_function='EXECUTE_PQL' AND INSTR(event_limit, start_mode)>0")
                        .runPQL(TaskStatus.SLOW)
            }

            //close job if expired
            dh.openQross()
                .get(s"SELECT id FROM qross_jobs WHERE enabled='yes' AND closing_time<>'' AND TIMESTAMPDIFF(MINUTE, '${now.getString("yyyy-MM-dd HH:mm:00")}', closing_time)<=1")
                    .put("UPDATE qross_jobs SET enabled='no', next_tick='N/A', closing_time='' WHERE id=#id")
                    .foreach(row => {
                        writeDebugging("Job " + row.getString("id") + " has been closed.")
                    })
                //open job if arrived
                .get(s"SELECT id, opening_time, cron_exp, next_tick FROM qross_jobs WHERE enabled='no' AND opening_time<>'' AND TIMESTAMPDIFF(MINUTE, '${now.getString("yyyy-MM-dd HH:mm:00")}', opening_time)<=1")
                .foreach(row => {
                    row.set("next_tick", ChronExp(row.getString("cron_exp")).getNextTickOrNone(row.getDateTime("opening_time")))
                    writeDebugging("Job " + row.getString("id") + " is opening.")
                })
                .put("UPDATE qross_jobs SET enabled='yes', next_tick='#next_tick', opening_time='' WHERE id=#id")
        }
        
        dh.close()
    }
}
