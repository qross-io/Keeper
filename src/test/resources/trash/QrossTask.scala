import io.qross.model.TaskRecorder
import io.qross.util.{DataHub, DateTime}

def restartTask(jobId: Int, taskId: Long, recordTime: String): DataHub = {

    //            .foreach(row => {
    //                row.set("to_be_start_time", DateTime.now.plusMinutes(row.getInt("event_value", 30)).getString("yyyyMMddHHmm00"))
    //            }).put(s"UPDATE qross_tasks SET to_be_start_time='#to_be_start_time' WHERE id=$taskId")

    if (dh.nonEmpty) {
        var delay = 30 //minute
        dh.foreach(row => {
            delay = row.getInt("event_value", 30)
            row.set("to_be_start_time", DateTime.now.plusMinutes(delay).getString("yyyyMMddHHmm00"))
        }).put(s"UPDATE qross_tasks SET to_be_start_time='#to_be_start_time' WHERE id=$taskId")
                .put("")

        if (recordTime == "") println("@X E")
        TaskRecorder.of(jobId, taskId, recordTime).debug(s"Task $taskId of job $jobId at <$recordTime> will restart after $delay minutes.").dispose()
    }

    dh.clear()
}

//complementsTasks
//update all jobs recent_tasks_status
dh.openDefault()
        .get(s"SELECT id AS job_id FROM qross_jobs")
        .pass("SELECT job_id, GROUP_CONCAT(CONCAT(id, ':', status, '@', task_time) ORDER BY id DESC SEPARATOR ',') AS status FROM (SELECT job_id, id, status, task_time FROM qross_tasks WHERE job_id=#job_id ORDER BY id DESC LIMIT 3) T GROUP BY job_id")
        .put("UPDATE qross_jobs SET recent_tasks_status='#status' WHERE id=#job_id")

//checkTasksStatus
//recent tasks status
dh.openDefault()
        .get(s"""SELECT job_id FROM qross_tasks WHERE update_time>='${DateTime(tick).minusMinutes(5).getString("yyyy-MM-dd HH:mm:ss")}'
                UNION SELECT id AS job_id FROM qross_jobs WHERE recent_tasks_status IS NULL""")
        .pass("SELECT job_id, GROUP_CONCAT(CONCAT(id, ':', status, '@', task_time) ORDER BY id DESC SEPARATOR ',') AS status FROM (SELECT job_id, id, status, task_time FROM qross_tasks WHERE job_id=#job_id ORDER BY id DESC LIMIT 3) T GROUP BY job_id")
        .put("UPDATE qross_jobs SET recent_tasks_status='#status' WHERE id=#job_id")