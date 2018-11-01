package io.qross.trash

class Trash {

    //case ANY => 2018.10.29 remove, merge into PARTIAL
    //dependencies
    //dags
    //dh.get(s"SELECT id, upstream_ids FROM qross_jobs_dags WHERE job_id=$jobId")
    //    .put(s"UPDATE qross_tasks_dags SET upstream_ids='#upstream_ids',status='waiting' WHERE task_id=$taskId AND command_id=#id")
    //val commandIds = option.drop(1)
    //dh.get(s"SELECT id FROM qross_jobs_dags WHERE job_id=$jobId WHERE id NOT IN ($commandIds)")
    //    .put(s"UPDATE qross_tasks_dags SET upstream_ids=REPLACE(upstream_ids, '(#id)', '') WHERE task_id=$taskId")
    /* 2018/09/11 so tedious, maybe correct
    while (dh.get(s"SELECT id, command_id FROM qross_tasks_dags WHERE task_id=$taskId AND upstream_ids='' AND status='waiting' AND command_id NOT IN ($commandIds)").nonEmpty) {
        dh.put(s"UPDATE qross_tasks_dags SET upstream_ids=REPLACE(upstream_ids, '(#command_id)', '') WHERE task_id=$taskId")
            .put("UPDATE qross_tasks_dags SET status='done' WHERE id=#id")
    }*/

    //TaskStarter - beat()
    /*
    def getManualCommandsToExecute(tick: String): DataTable = {
        val minute = DateTime(tick)
        val ds = new DataSource()
        //commands
        val executable = ds.executeDataTable(commandsBaseSQL
            + " WHERE B.status IN ('finished', 'failed', 'incorrect') AND A.status IN ('restarting', 'manual')")

        writeMessage("TaskStarter beat!")
        ds.executeNonQuery(s"UPDATE qross_keeper_beats SET last_beat_time=NOW() WHERE actor_name='TaskStarter'")
        ds.close()

        executable
    }
    def checkOvertimeOfActions(tick: String): Unit = {
        val minute = DateTime(tick)
        val dh = new DataHub()

        dh.get("SELECT A.id AS action_id, A.job_id, A.task_id, A.command_id, B.command_text, B.overtime, C.title, C.owner, C.mail_notification, C.mail_master_on_exception, D.task_time FROM qross_tasks_dags A INNER JOIN qross_jobs_dags B ON A.status='running' AND A.job_id=B.job_id AND B.overtime>0 AND TIMESTAMPDIFF(SECOND, A.update_time, NOW())>B.overtime INNER JOIN qross_jobs C ON A.job_id=C.id AND C.enabled='yes' INNER JOIN qross_tasks D ON A.task_id=D.id")
        if (dh.nonEmpty) {
            dh.put("UPDATE qross_tasks_dags SET status='timeout' WHERE id=#action_id")
                .put("UPDATE qross_tasks SET status='timeout', checked='no' WHERE id=#task_id")

            if (Global.EMAIL_NOTIFICATION) {
                dh.foreach(row => {
                    if (row.getBoolean("mail_notification") && row.getString("owner", "") != "") {
                        OpenResourceFile("/templates/timeout.html")
                            .replaceWith(row)
                            .writeEmail(s"TIMEOUT: ${row.getString("title")} ${row.getString("task_time")} - TaskID: ${row.getString("task_id")}")
                            .to(row.getString("owner"))
                            .cc(if (row.getBoolean("mail_master_on_exception")) Global.MASTER_USER_GROUP else "")
                            .send()
                    }
                })
            }
        }

        writeMessage("TaskStarter beat!")
        dh.set(s"UPDATE qross_keeper_beats SET last_beat_time=NOW() WHERE actor_name='TaskStarter'")
        dh.close()
    }
    */

}
