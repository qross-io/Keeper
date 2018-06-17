package io.qross.model

import io.qross.util.Output._
import io.qross.util._

import scala.collection.mutable

object Global {
    
    val CONFIG = DataRow()
    DataSource.queryDataTable("SELECT conf_key, conf_value FROM qross_conf")
        .foreach(row => {
            CONFIG.set(row.getString("conf_key"), row.getString("conf_value"))
        }).clear()
    CONFIG.set("MASTER_USER_GROUP", User.getUsers("master"))
    CONFIG.set("KEEPER_USER_GROUP", User.getUsers("keeper"))
    
    def QROSS_VERSION: String = CONFIG.getString("QROSS_VERSION", "")
    val CORES: Int = Runtime.getRuntime.availableProcessors
    def USER_HOME: String = System.getProperty("user.dir")
    def QROSS_HOME: String = {
        CONFIG.getString("QROSS_HOME").replace("\\", "/").replace("%USER_HOME_DIR", USER_HOME).replace("//", "/")
    }
    def QROSS_WORKER_HOME: String = {
        CONFIG.getString("QROSS_WORKER_HOME").replace("\\", "/").replace("%QROSS_HOME", QROSS_HOME).replace("%USER_HOME_DIR", USER_HOME).replace("//", "/")
    }
    def QROSS_KEEPER_HOME: String = {
        CONFIG.getString("QROSS_KEEPER_HOME").replace("\\", "/").replace("%QROSS_HOME", QROSS_HOME).replace("%USER_HOME_DIR", USER_HOME).replace("//", "/")
    }
    def JAVA_BIN_HOME: String = CONFIG.getString("JAVA_BIN_HOME")
    def EMAIL_NOTIFICATION: Boolean = CONFIG.getBoolean("EMAIL_NOTIFICATION")
    def QUIT_ON_NEXT_BEAT: Boolean = CONFIG.getBoolean("QUIT_ON_NEXT_BEAT")
    def MASTER_USER_GROUP: String = CONFIG.getString("MASTER_USER_GROUP")
    def KEEPER_USER_GROUP: String = CONFIG.getString("KEEPER_USER_GROUP")
    //def CLEAN_TASK_RECORDS_FREQUENCY: String = CONFIG.getString("CLEAN_TASK_RECORDS_FREQUENCY")
    //def BEATS_MAILING_FREQUENCY: String = CONFIG.getString("BEATS_MAILING_FREQUENCY")
   
    //var CLEAN_TASK_RECORDS_FREQUENCY = "0 5 0/6 * * ? *"
    //var BEATS_MAILING_FREQUENCY = "0 0 3,9,12,15,18 * * ? *"
    /*
    def runSystemTasks(tick: String): Unit = {
        val minute = DateTime(tick)
        
        if (CronExp(CLEAN_TASK_RECORDS_FREQUENCY).matches(minute)) {
            clearTaskRecords()
        }
        
        if (EMAIL_NOTIFICATION && CronExp(BEATS_MAILING_FREQUENCY).matches(minute)) {
            sendBeatsMail(minute)
        }
        
        DataSource.queryUpdate(s"UPDATE qross_keeper_beats SET last_beat_time='${minute.getString("yyyy-MM-dd HH:mm:ss")}' WHERE actor_name='GlobalController';")
        writeMessage("GlobalController beat!")
    }*/
    
    def clearTaskRecords(): Unit = {
        val dh = new DataHub()
        dh.openDefault().saveAsDefault()
            .get("SELECT id, keep_x_task_records AS job_id FROM qross_jobs WHERE keep_x_task_records>0")
            .pass("SELECT job_id, #keeper_x_task_records AS keeper_x_task_records FROM qross_tasks WHERE job_id=#id GROUP BY job_id HAVING COUNT(0)>#keeper_x_task_records")
            .pass("SELECT id AS task_id, job_id FROM qross_tasks WHERE job_id=#job_id ORDER BY id DESC LIMIT #keeper_x_task_records,1")
            .put("DELETE FROM qross_tasks WHERE job_id=#job_id AND id<=#task_id")
            .put("DELETE FROM qross_tasks_logs WHERE job_id=#job_id AND task_id<=#task_id")
            .put("DELETE FROM qross_tasks_dependencies WHERE job_id=#job_id AND task_id<=#task_id")
            .put("DELETE FROM qross_tasks_dags WHERE job_id=#job_id AND task_id<=#task_id")
        dh.close()
    
        writeMessage("Task records cleaned!")
    }
    
    def sendBeatsMail(): Unit = {
        val ds = new DataSource()
        val tick = DateTime.now.getString("yyyy-MM-dd HH:mm:ss")
        
        if (Global.KEEPER_USER_GROUP != "" || Global.MASTER_USER_GROUP != "") {
            OpenResourceFile("/templates/beats.html")
                .replace("${tick}", tick)
                .replace("${beats}", Beats.toHtml(ds.executeDataTable("SELECT actor_name, status, last_beat_time FROM qross_keeper_beats WHERE status<>'disabled' ORDER BY id DESC")))
                .writeEmail(s"Keeper Beats at $tick")
                .to(if (Global.KEEPER_USER_GROUP != "") Global.KEEPER_USER_GROUP else Global.MASTER_USER_GROUP)
                .cc(if (Global.KEEPER_USER_GROUP != "" ) Global.MASTER_USER_GROUP else "")
                .send()
        }
        ds.close()
        
        writeMessage("Beats mail sent!")
    }
    
    def recordStart(): Unit = {
        val ds = new DataSource()
        val method =
            if (ds.executeExists("SELECT id FROM qross_keeper_beats WHERE actor_name='keeper' AND status='rest'")) {
                "manual"
            }
            else {
                "crontab"
            }
        
        ds.executeNonQuery(s"INSERT INTO qross_keeper_start_records (method) VALUES ('$method')")
       
        if (method != "manual" && Global.MASTER_USER_GROUP != "") {
            Email.write(s"RESTART: Keeper start by <$method> at " + DateTime.now.getString("yyyy-MM-dd HH:mm:ss")).to(Global.MASTER_USER_GROUP).send()
        }
        
        ds.close()
    }
}
