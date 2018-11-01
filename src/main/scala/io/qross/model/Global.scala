package io.qross.model

import io.qross.util.Output._
import io.qross.util._

object Global {
    
    val CONFIG = DataRow()
    DataSource.queryDataTable("SELECT conf_key, conf_value FROM qross_conf")
        .foreach(row => {
            CONFIG.set(row.getString("conf_key"), row.getString("conf_value"))
        }).clear()
    CONFIG.set("MASTER_USER_GROUP", QrossUser.getUsers("master"))
    CONFIG.set("KEEPER_USER_GROUP", QrossUser.getUsers("keeper"))
    
    def QROSS_VERSION: String = CONFIG.getString("QROSS_VERSION")
    
    def COMPANY_NAME: String = CONFIG.getString("COMPANY_NAME")
    
    val CORES: Int = Runtime.getRuntime.availableProcessors
    
    def USER_HOME: String = FilePath.format(System.getProperty("user.dir"))
    
    def QROSS_HOME: String = FilePath.format(CONFIG.getString("QROSS_HOME")).replace("%USER_HOME", USER_HOME).replace("//", "/")
    
    def QROSS_WORKER_HOME: String = FilePath.format(CONFIG.getString("QROSS_WORKER_HOME")).replace("%QROSS_HOME", QROSS_HOME).replace("%USER_HOME", USER_HOME).replace("//", "/")
    
    def QROSS_KEEPER_HOME: String = FilePath.format(CONFIG.getString("QROSS_KEEPER_HOME")).replace("%QROSS_HOME", QROSS_HOME).replace("%USER_HOME", USER_HOME).replace("//", "/")
    
    def JAVA_BIN_HOME: String = CONFIG.getString("JAVA_BIN_HOME")

    def PYTHON2_HOME: String = CONFIG.getString("PYTHON2_HOME")

    def PYTHON3_HOME: String = CONFIG.getString("PYTHON3_HOME")
    
    def EMAIL_NOTIFICATION: Boolean = CONFIG.getBoolean("EMAIL_NOTIFICATION")
    
    def HADOOP_AND_HIVE_ENABLED: Boolean = CONFIG.getBoolean("HADOOP_AND_HIVE_ENABLED")
    
    def LOGS_LEVEL: String = CONFIG.getString("LOGS_LEVEL", "DEBUG").toUpperCase
    
    def CONCURRENT_BY_CPU_CORES: Int = CONFIG.getInt("CONCURRENT_BY_CPU_CORES", 4)
    
    def EMAIL_EXCEPTIONS_TO_DEVELOPER: Boolean = CONFIG.getBoolean("EMAIL_EXCEPTIONS_TO_DEVELOPER")
    
    def QUIT_ON_NEXT_BEAT: Boolean = CONFIG.getBoolean("QUIT_ON_NEXT_BEAT")
    
    def MASTER_USER_GROUP: String = CONFIG.getString("MASTER_USER_GROUP")
    
    def KEEPER_USER_GROUP: String = CONFIG.getString("KEEPER_USER_GROUP")

    def CHARSET: String = CONFIG.getString("CHARSET")

    def API_ON_TASK_CHECKING_LIMIT: String = CONFIG.getString("API_ON_TASK_CHECKING_LIMIT")

    def API_ON_TASK_FAILED: String = CONFIG.getString("API_ON_TASK_FAILED")

    def API_ON_TASK_INCORRECT: String = CONFIG.getString("API_ON_TASK_INCORRECT")

    def API_ON_TASK_TIMEOUT: String = CONFIG.getString("API_ON_TASK_TIMEOUT")

    def API_ON_TASK_FINISHED: String = CONFIG.getString("API_ON_TASK_FINISHED")
    
    //def CLEAN_TASK_RECORDS_FREQUENCY: String = CONFIG.getString("CLEAN_TASK_RECORDS_FREQUENCY")
    def BEATS_MAILING_FREQUENCY: String = CONFIG.getString("BEATS_MAILING_FREQUENCY")
    
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
        
        DataSource.queryUpdate(s"UPDATE qross_keeper_beats SET last_beat_time=NOW() WHERE actor_name='GlobalController';")
        writeMessage("GlobalController beat!")
    }*/
    
    def clearTaskRecords(): Unit = {
        val dh = new DataHub()
        dh.openDefault()
            .get("SELECT id, keep_x_task_records FROM qross_jobs WHERE keep_x_task_records>0")
            .pass("SELECT job_id, #keep_x_task_records AS keep_x_task_records FROM qross_tasks WHERE job_id=#id GROUP BY job_id HAVING COUNT(0)>#keep_x_task_records", "id" -> 0, "keep_x_task_records" -> 100)
            .pass("SELECT id AS task_id, job_id FROM qross_tasks WHERE job_id=#job_id ORDER BY id DESC LIMIT #keep_x_task_records,1", "job_id" -> 0, "keep_x_task_records" -> 100)
            .put("DELETE FROM qross_tasks WHERE job_id=#job_id AND id<=#task_id")
            .put("DELETE FROM qross_tasks_logs WHERE job_id=#job_id AND task_id<=#task_id")
            .put("DELETE FROM qross_tasks_dependencies WHERE job_id=#job_id AND task_id<=#task_id")
            .put("DELETE FROM qross_tasks_dags WHERE job_id=#job_id AND task_id<=#task_id")
        dh.close()
        
        writeMessage("Task records cleaned!")
    }
    
}
