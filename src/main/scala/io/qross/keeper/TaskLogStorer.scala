package io.qross.keeper

import io.qross.core.DataHub
import io.qross.ext.Output.writeDebugging
import io.qross.time.DateTime

//to be removed

object TaskLogStorer {
    def main(args: Array[String]): Unit = {

        val dh = DataHub.QROSS
        writeDebugging("Storing logs mechanism is ready to execute.")

        dh.get(s"""SELECT job_id, id AS task_id, create_time FROM qross_tasks WHERE update_time<'${DateTime.now.minusDays(1).getString("yyyy-MM-dd HH:mm:00")}' AND saved='no'""")
            .foreach(row => {
//                TaskStorage.store(
//                    row.getInt("job_id"),
//                    row.getLong("task_id"),
//                    row.getString("create_time"),
//                    dh)
            })
            .put("DELETE FROM qross_tasks_logs WHERE job_id=#job_id AND task_id=#task_id")
            .put("UPDATE qross_tasks SET saved='yes' WHERE id=#task_id")

        writeDebugging("Logs of " + dh.COUNT_OF_LAST_GET + " tasks has been stored.")

        dh.close()
    }
}
