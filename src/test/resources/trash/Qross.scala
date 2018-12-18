import io.qross.model.{Global, TaskOverall}
import io.qross.util.{DataSource, OpenResourceFile}

val ds = new DataSource()
if (ds.executeExists("SELECT id FROM qross_keeper_running_records WHERE id=(SELECT id FROM qross_keeper_running_records ORDER BY id DESC LIMIT 1) AND status='running'")) {

    var error = ""
    var toSend = true
    var title = ""

    val actors = ds.executeDataTable("SELECT actor_name FROM qross_keeper_beats WHERE actor_name IN ('Keeper', 'Messager', 'TaskProducer', 'TaskStarter', 'TaskChecker', 'TaskExecutor', 'TaskLogger') AND status='rest'")
    if (actors.nonEmpty) {
        //quit system and auto restart
        ds.executeNonQuery("INSERT INTO qross_message_box (message_type, message_key, message_text) VALUES ('GLOBAL', 'QUIT_ON_NEXT_BEAT', 'yes');")
        //mail info
        error = "Keeper will restart on next tick. Please wait util all executing task finished."
        title = s"Keeper Beats Exception: ${actors.mkString(", ", "actor_name")} at $tick"
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
        OpenResourceFile("/templates/beats.html")
                .replace("${tick}", tick)
                .replace("${error}", error)
                .replace("${beats}", ds.executeDataTable("SELECT actor_name, status, last_beat_time FROM qross_keeper_beats ORDER BY id DESC").mkString("", " - ", "<br/>"))
                .writeEmail(title)
                .to(if (Global.KEEPER_USER_GROUP != "") Global.KEEPER_USER_GROUP else Global.MASTER_USER_GROUP)
                .cc(if (Global.KEEPER_USER_GROUP != "") Global.MASTER_USER_GROUP else "")
                .send()
    }

    //close job if expire
    ds.executeDataTable(s"SELECT id FROM qross_jobs WHERE enabled='yes' AND closing_time='${now.getString("yyyyMMddHHmm00")}'")
            .updateSource("UPDATE qross_jobs SET enabled='no', next_tick='N/A' WHERE id=#id")

    //clean mechanism
    if (now.matches(Global.CLEAN_TASK_RECORDS_FREQUENCY)) {

        ds.executeDataTable("""SELECT A.job_id, A.method, A.to_keep_records, B.task_records
                   FROM (SELECT job_id, event_value AS method, event_option AS to_keep_records FROM qross_jobs_events WHERE event_name='onJobClean' AND event_function='CLEAN_TASK_RECORDS') A
                   INNER JOIN (SELECT job_id, COUNT(0) AS task_records FROM qross_tasks GROUP BY job_id) B ON A.job_id=B.job_id AND task_records>to_keeper_records""")
                .foreach(row => {
                    val jobId = row.getInt("jobId")
                    val toKeepRecords = row.getInt("to_keep_records")
                    val taskId = ds.executeSingleValue(s"""SELECT id AS task_id FROM qross_tasks WHERE job_id=$jobId LIMIT $toKeepRecords,1""").getOrElse("0").toLong
                    val method = row.getString("method").toLowerCase()

                    if (taskId > 0) {
                        //backup records
                        if (method == "backup") {
                            ds.executeDataTable(s"SELECT id FROM qross_tasks WHERE job_id=$jobId AND id<=$taskId")
                                    .foreach(row => {
                                        TaskOverall.of(row.getLong("id")).store()
                                    })
                        }

                        //clean database
                        ds.executeNonQuery(s"DELETE FROM qross_tasks_logs WHERE job_id=$jobId AND task_id<=$taskId")
                        ds.executeNonQuery(s"DELETE FROM qross_tasks_dependencies WHERE job_id=$jobId AND task_id<=$taskId")
                        ds.executeNonQuery(s"DELETE FROM qross_tasks_dags WHERE job_id=$jobId AND task_id<=$taskId")
                        ds.executeNonQuery(s"DELETE FROM qross_tasks WHERE job_id=$jobId AND id<=$taskId")
                    }
                }).clear()
    }
}

ds.close()