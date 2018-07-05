package io.qross.model

import io.qross.util.Output.{writeDebugging, writeMessage}
import io.qross.util._

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
        DataSource.queryUpdate(s"UPDATE qross_keeper_beats SET last_beat_time='${DateTime.now.getString("yyyy-MM-dd HH:mm:ss")}' WHERE actor_name='$actor'")
    }
    
    def quit(actor: String): Unit = {
        writeDebugging(if (actor != "Keeper") s"$actor quit!" else "Qross Keeper shut down!")
        DataSource.queryUpdate(s"UPDATE qross_keeper_beats SET status='rest', quit_time=NOW() WHERE actor_name='$actor'")
        
    }
    
    def stop(): Unit = {
        val dh = new DataHub()
        dh.get("SELECT id FROM qross_keeper_running_records ORDER BY id DESC LIMIT 1")
            .put("UPDATE qross_keeper_running_records SET status='stopping', stop_time=NOW(), duration=TIMESTAMPDIFF(SECOND, start_time, NOW()) WHERE id=#id")
        dh.close()
    }
    
    def checkBeats(): Unit = {
        
        val ds = new DataSource()
        if (ds.executeExists("SELECT id FROM qross_keeper_running_records WHERE id=(SELECT id FROM qross_keeper_running_records ORDER BY id DESC LIMIT 1) AND status='running'")) {
            val tick = DateTime.now.getString("yyyy-MM-dd HH:mm:ss")
            var error = ""
            var doSend = true
            var title = ""
            
            val actors = ds.executeDataTable("SELECT actor_name FROM qross_keeper_beats WHERE actor_name IN ('Keeper', 'Messager', 'TaskProducer', 'TaskStarter', 'TaskChecker', 'TaskExecutor', 'TaskLogger') AND status='rest'")
            if (actors.nonEmpty) {
                //quit system and auto restart
                ds.executeNonQuery("INSERT INTO qross_message_box (message_type, message_key, message_text) VALUES ('GLOBAL', 'QUIT_ON_NEXT_BEAT', 'yes');")
                //mail info
                error = "Keeper will restart on next tick. Please wait util all executing task finished."
                title = s"Beats Exception: ${actors.mkString(", ", "actor_name")} at $tick"
            }
            else if (DateTime.now.setSecond(0).matches(Global.BEATS_MAILING_FREQUENCY)) {
                //regular
                title = s"Beats regularly at $tick"
            }
            else {
                doSend = false
            }
            
            if (doSend && (Global.KEEPER_USER_GROUP != "" || Global.MASTER_USER_GROUP != "")) {
                OpenResourceFile("/templates/beats.html")
                    .replace("${tick}", tick)
                    .replace("${error}", error)
                    .replace("${beats}", ds.executeDataTable("SELECT actor_name, status, last_beat_time FROM qross_keeper_beats WHERE status<>'disbaled' ORDER BY id DESC").mkString("", " - ", "<br/>"))
                    .writeEmail(title)
                    .to(if (Global.KEEPER_USER_GROUP != "") Global.KEEPER_USER_GROUP else Global.MASTER_USER_GROUP)
                    .cc(if (Global.KEEPER_USER_GROUP != "") Global.MASTER_USER_GROUP else "")
                    .send()
            }
        }
        
        ds.close()
    }
}
