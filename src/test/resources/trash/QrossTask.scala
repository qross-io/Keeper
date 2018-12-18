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