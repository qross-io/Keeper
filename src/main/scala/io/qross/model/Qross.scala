package io.qross.model

import io.qross.core.DataHub
import io.qross.ext.Output._
import io.qross.fs.ResourceFile
import io.qross.jdbc.DataSource
import io.qross.net.Email
import io.qross.setting.Global
import io.qross.time.{DateTime, Timer}

object Qross {
    
    def start(): Unit = {
        val ds = new DataSource()
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
        DataSource.queryUpdate(s"UPDATE qross_keeper_beats SET status='running',start_time=NOW() WHERE actor_name='$actor'")
    }
    
    def beat(actor: String): Unit = {
        writeMessage(actor + " beat!")
        DataSource.queryUpdate(s"UPDATE qross_keeper_beats SET last_beat_time=NOW() WHERE actor_name='$actor'")
    }
    
    def quit(actor: String): Unit = {
        writeDebugging(s"$actor quit!")
        DataSource.queryUpdate(s"UPDATE qross_keeper_beats SET status='rest', quit_time=NOW() WHERE actor_name='$actor'")
        
    }
    
    def waitAndStop(): Unit = {
        val dh = new DataHub()
        dh.get("SELECT id FROM qross_keeper_running_records ORDER BY id DESC LIMIT 1")
            .put("UPDATE qross_keeper_running_records SET status='stopping', stop_time=NOW(), duration=TIMESTAMPDIFF(SECOND, start_time, NOW()) WHERE id=#id")

        while(dh.get("SELECT actor_name FROM qross_keeper_beats WHERE status='running' LIMIT 1").nonEmpty) {
            writeDebugging(dh.firstRow.getString("actor_name") + " still working.")
            dh.clear()
            dh.get("SELECT COUNT(0) AS amount FROM qross_tasks WHERE status='executing'")
            writeDebugging("There is " + dh.firstRow.getString("amount") + " tasks still executing.")
            dh.clear()
            Timer.sleep(5F)
        }

        dh.close()
    }
    
    def checkBeatsAndRecords(): Unit = {

        val now = DateTime.now
        val tick = now.getString("yyyy-MM-dd HH:mm:ss")
        now.setSecond(0)

        val dh = new DataHub()
        if (dh.executeExists("SELECT id FROM qross_keeper_running_records WHERE id=(SELECT id FROM qross_keeper_running_records ORDER BY id DESC LIMIT 1) AND status='running'")) {

            var error = ""
            var toSend = true
            var title = ""
            
            dh.get("SELECT actor_name FROM qross_keeper_beats WHERE actor_name IN ('Keeper', 'Messager', 'TaskProducer', 'TaskStarter', 'TaskChecker', 'TaskExecutor', 'TaskLogger') AND status='rest'")
            if (dh.nonEmpty) {
                //quit system and auto restart
                dh.set("INSERT INTO qross_message_box (message_type, message_key, message_text) VALUES ('GLOBAL', 'QUIT_ON_NEXT_BEAT', 'yes');")
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
                    .replace("${tick}", tick)
                    .replace("${error}", error)
                    .replace("${beats}", dh.executeDataTable("SELECT CONCAT(actor_name, ' - ', status, ' - ', last_beat_time) AS info FROM qross_keeper_beats ORDER BY id DESC").getColumn("info").mkString("<br/>"))
                    .writeEmail(title)
                    .to(if (Global.KEEPER_USER_GROUP != "") Global.KEEPER_USER_GROUP else Global.MASTER_USER_GROUP)
                    .cc(if (Global.KEEPER_USER_GROUP != "") Global.MASTER_USER_GROUP else "")
                    .send()
            }

            //close job if expire
            dh.clear()
                .get(s"SELECT id FROM qross_jobs WHERE enabled='yes' AND closing_time='${now.getString("yyyyMMddHHmm00")}'")
                    .put("UPDATE qross_jobs SET enabled='no', next_tick='N/A' WHERE id=#id")
                    .foreach(row => {
                        writeMessage("Job " + row.getString("id") + " has been closed.")
                    })

            //clean mechanism
            if (now.matches(Global.CLEAN_TASK_RECORDS_FREQUENCY)) {

                dh.get("""SELECT A.job_id, A.method, A.to_keep_records, B.task_records, (B.task_records-A.to_keep_records) AS offset
                   FROM (SELECT job_id, event_value AS method, event_option AS to_keep_records FROM qross_jobs_events WHERE event_name='onJobClean' AND event_function='CLEAN_TASK_RECORDS') A
                   INNER JOIN (SELECT job_id, COUNT(0) AS task_records FROM qross_tasks GROUP BY job_id) B ON A.job_id=B.job_id AND B.task_records>A.to_keep_records""")
                        .foreach(row => {
                            val jobId = row.getInt("job_id")
                            val offset = row.getInt("offset")
                            val taskId = dh.executeSingleValue(s"""SELECT id AS task_id FROM qross_tasks WHERE job_id=$jobId ORDER BY id ASC LIMIT $offset,1""").getOrElse(0).asInstanceOf[Long]

                            if (taskId > 0) {
                                val method = row.getString("method").toLowerCase()
                                var rows = 0

                                //backup records
                                if (method == "store") {
                                    dh.executeDataTable(s"SELECT id FROM qross_tasks WHERE job_id=$jobId AND id<$taskId")
                                            .foreach(row => {
                                                TaskOverall.of(row.getLong("id")).store()
                                            })
                                }

                                //clean database
                                dh.executeNonQuery(s"DELETE FROM qross_tasks_logs WHERE job_id=$jobId AND task_id<$taskId")
                                dh.executeNonQuery(s"DELETE FROM qross_tasks_dependencies WHERE job_id=$jobId AND task_id<$taskId")
                                dh.executeNonQuery(s"DELETE FROM qross_tasks_dags WHERE job_id=$jobId AND task_id<$taskId")
                                dh.executeNonQuery(s"DELETE FROM qross_tasks_events WHERE job_id=$jobId AND task_id<$taskId")
                                dh.executeNonQuery(s"DELETE FROM qross_tasks_records WHERE job_id=$jobId AND task_id<$taskId")
                                rows = dh.executeNonQuery(s"DELETE FROM qross_tasks WHERE job_id=$jobId AND id<$taskId")
                                dh.executeNonQuery(s"INSERT INTO qross_jobs_clean_records (job_id, method, amount) VALUES ($jobId, '$method', $rows)")

                                writeDebugging(s"$rows tasks of job $jobId has been ${method}d.")
                            }
                        }).clear()
            }
        }
        
        dh.close()
    }
}
