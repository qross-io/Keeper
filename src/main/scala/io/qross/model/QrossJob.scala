package io.qross.model

import io.qross.ext.Output
import io.qross.jdbc.DataSource
import io.qross.keeper.Keeper
import io.qross.net.Http
import io.qross.setting.Global

import scala.collection.mutable

object QrossJob {

    def deleteLogs(jobId: Int): Unit = {
        DataSource.QROSS.queryDataTable(s"SELECT task_id, record_time FROM qross_tasks_records WHERE job_id=$jobId AND node_address=${Keeper.NODE_ADDRESS} UNION ALL SELECT id AS task_id, record_time FROM qross_tasks WHERE job_id=$jobId AND node_address=${Keeper.NODE_ADDRESS}")
            .foreach(row => {
                val datetime = row.getDateTime("record_time")
                new java.io.File(s"""${Global.QROSS_HOME}/tasks/${datetime.getString("yyyyMMdd")}/$jobId/${row.getLong("task_id")}_${datetime.getString("HHmmss")}.log""").delete()
            }).clear()
    }


    def killJob(jobId: Int, killer: Int = 0): String = {

        Output.writeDebugging(s"All tasks of job $jobId will be killed.")

        val address = Keeper.NODE_ADDRESS

        val actions = DataSource.QROSS.queryDataTable("SELECT A.id, A.task_id, B.node_address FROM qross_tasks_dags A INNER JOIN qross_tasks B ON A.task_id=B.id WHERE A.job_id=? AND A.status='running'", jobId)
        val killing = new mutable.HashMap[Long, String]()
        actions.foreach(row => {
            val actionId = row.getLong("id")
            if (row.getString("node_address") == address) {
                if (QrossTask.EXECUTING.contains(actionId)) {
                    QrossTask.TO_BE_KILLED += actionId -> killer
                }
            }
            else {
                killing += row.getLong("task_id") -> row.getString("node_address")
            }
        })

        killing.foreach(task => {
            try {
                Http.PUT(s"""http://${task._2}/task/kill/${task._1}?killer=$killer""").request()
            }
            catch {
                case e: Exception =>
                    e.printStackTrace()
                    Qross.disconnect(task._2)
            }
        })

        if (actions.nonEmpty) {
            s"""{ "actions": [${actions.firstColumn.mkString(", ")}] }"""
        }
        else {
            s"""{ "actions": [] }"""
        }
    }
}
