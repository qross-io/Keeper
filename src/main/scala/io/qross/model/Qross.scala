package io.qross.model

import io.qross.core.DataHub
import io.qross.ext.Output._
import io.qross.fs.{Directory, ResourceFile}
import io.qross.jdbc.DataSource
import io.qross.net.Email
import io.qross.pql.PQL
import io.qross.pql.PQL._
import io.qross.setting.Global
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
        
        if (method != "manual" && Global.MASTER_USER_GROUP != "") {
            Email.write(s"RESTART: Keeper start by <$method> at " + DateTime.now.getString("yyyy-MM-dd HH:mm:ss")).to(Global.MASTER_USER_GROUP).send()
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
        if (dh.executeExists("SELECT id FROM qross_keeper_running_records WHERE id=(SELECT id FROM qross_keeper_running_records ORDER BY id DESC LIMIT 1) AND status='running'")) {

            var error = ""
            var toSend = true
            var title = ""

            //irregular - will restart Keeper
            dh.get("SELECT actor_name FROM qross_keeper_beats WHERE actor_name IN ('Keeper', 'Messenger', 'TaskProducer', 'TaskStarter', 'TaskChecker', 'TaskExecutor', 'TaskLogger') AND status='rest'")
            if (dh.nonEmpty) {
                //quit system and auto restart
                dh.set("INSERT INTO qross_message_box (message_type, message_key, message_text) VALUES ('GLOBAL', 'QUIT_ON_NEXT_BEAT', 'yes')")
                //mail info
                error = "Keeper will restart on next tick. Please wait util all executing task finished."
                title = s"Keeper Beats Exception: ${dh.getColumn("actor_name").mkString(",")} at $tick"

                writeWarning(error)
            }
            else if (now.matches(Global.BEATS_MAILING_FREQUENCY)) {
                //regular
                title = s"Keeper Beats regularly at $tick"
            }
            else {
                toSend = false
            }

            //send beats mail
            if (toSend && (Global.KEEPER_USER_GROUP != "" || Global.MASTER_USER_GROUP != "")) {
                ResourceFile.open("/templates/beats.html")
                    .replace("#{tick}", tick)
                    .replace("#{error}", error)
                    .replace("#{beats}", dh.executeDataTable("SELECT CONCAT(actor_name, ' - ', status, ' - ', last_beat_time) AS info FROM qross_keeper_beats ORDER BY id DESC").getColumn("info").mkString("<br/>"))
                    .writeEmail(title)
                    .to(if (Global.KEEPER_USER_GROUP != "") Global.KEEPER_USER_GROUP else Global.MASTER_USER_GROUP)
                    .cc(if (Global.KEEPER_USER_GROUP != "") Global.MASTER_USER_GROUP else "")
                    .send()

                writeDebugging("Beats mail has been sent.")
            }

            //close job if expire
            dh.clear()
                .get(s"SELECT id FROM qross_jobs WHERE enabled='yes' AND closing_time<>'' AND TIMESTAMPDIFF(MINUTE, '${now.getString("yyyy-MM-dd HH:mm:00")}', closing_time)<=1")
                    .put("UPDATE qross_jobs SET enabled='no', next_tick='N/A', closing_time='' WHERE id=#id")
                    .foreach(row => {
                        writeMessage("Job " + row.getString("id") + " has been closed.")
                    })
                //open job
                .get(s"SELECT id, opening_time, cron_exp, next_tick FROM qross_jobs WHERE enabled='no' AND opening_time<>'' AND TIMESTAMPDIFF(MINUTE, '${now.getString("yyyy-MM-dd HH:mm:00")}', opening_time)<=1")
                .foreach(row => {
                    row.set("next_tick", ChronExp(row.getString("cron_exp")).getNextTickOrNone(row.getDateTime("opening_time")))
                    writeMessage("Job " + row.getString("id") + " is opening.")
                })
                .put("UPDATE qross_jobs SET enabled='yes', next_tick='#next_tick', opening_time='' WHERE id=#id")
        }

        //store logs to disk every hour xx:27
        if (now.matches(Global.CLEAN_TASK_LOGS_FREQUENCY)) {
            writeDebugging("Storing logs mechanism is ready to execute.")
            dh.get(s"""SELECT job_id, id AS task_id, create_time FROM qross_tasks WHERE update_time<'${now.minusDays(1).getString("yyyy-MM-dd HH:mm:00")}' AND saved='no'""")
                    .foreach(row => {
                        TaskStorage.store(
                            row.getInt("job_id"),
                            row.getLong("task_id"),
                            row.getString("create_time"),
                            dh)
                        }
                    )
                    .put("DELETE FROM qross_tasks_logs WHERE task_id=#task_id")
                    .put("UPDATE qross_tasks SET saved='yes' WHERE id=#task_id")
            writeDebugging(dh.COUNT + " tasks has been stored.")
            writeDebugging("Storing logs mechanism has finished.")
        }

        //disk space monitor
        if (now.matches(Global.DISK_MONITOR_FREQUENCY)) {
            dh.set(s"INSERT INTO qross_space_monitor (moment, keeper_logs_space_usage, task_logs_space_usage, temp_space_usage) VALUES ('${now.getString("yyyy-MM-dd HH:mm:ss")}', ${Directory.spaceUsage(Global.QROSS_HOME + "logs/")}, ${Directory.spaceUsage(Global.QROSS_HOME + "tasks/")}, ${Directory.spaceUsage(Global.QROSS_HOME + "temp/")})")
            writeDebugging("Disk info has been saved.")
        }

        //clean mechanism
        if (now.matches(Global.CLEAN_TASK_RECORDS_FREQUENCY)) {
            writeDebugging("Cleaning tasks mechanism is ready to execute.")
            dh.run(
                """
                    VAR $TO_CLEAR := SELECT B.job_id, A.keep_x_task_records FROM qross_jobs A
                                        INNER JOIN (SELECT job_id, COUNT(0) AS task_amount FROM qross_tasks GROUP BY job_id) B ON A.id=B.job_id
                                            WHERE A.keep_x_task_records>0 AND B.task_amount>A.keep_x_task_records;
                    FOR $job_id, $keep_tasks IN $TO_CLEAR
                      LOOP
                        SET $task_id := SELECT id AS task_id FROM qross_tasks WHERE job_id=$job_id ORDER BY id DESC LIMIT $keep_tasks,1;

                        FOR $id, $create_time IN (SELECT id, create_time FROM qross_tasks WHERE job_id=$job_id AND id<=$task_id)
                          LOOP
                            DELETE FILE @QROSS_HOME + "tasks/" + $job_id + "/" + ${ $create_time REPLACE "-" TO "" SUBSTRING 1 TO 9 } + "/" + $id + ".log";
                          END LOOP;

                        DELETE FROM qross_tasks_logs WHERE job_id=$job_id AND task_id<=$task_id;
                        DELETE FROM qross_tasks_dependencies WHERE job_id=$job_id AND task_id<=$task_id;
                        DELETE FROM qross_tasks_dags WHERE job_id=$job_id AND task_id<=$task_id;
                        DELETE FROM qross_tasks_events WHERE job_id=$job_id AND task_id<=$task_id;
                        DELETE FROM qross_tasks_records WHERE job_id=$job_id AND task_id<=$task_id;
                        SET $rows := DELETE FROM qross_tasks WHERE job_id=$job_id AND id<=$task_id;
                        INSERT INTO qross_jobs_clean_records (job_id, amount, info) VALUES ($job_id, $rows, $task_id);

                        PRINT DEBUG $rows + ' tasks of job ' + $job_id + ' has been deleted.';
                      END LOOP;
                 """)
            writeDebugging("Tasks cleaning has finished.")
        }
        
        dh.close()
    }
}
