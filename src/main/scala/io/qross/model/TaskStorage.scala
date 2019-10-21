package io.qross.model

import java.io.File

import io.qross.core.DataTable
import io.qross.fs.{FileReader, FileWriter}
import io.qross.jdbc.DataSource
import io.qross.net.Json
import io.qross.setting.Global

object TaskStorage {

    //write logs to file
    def store(jobId: Int, taskId: Long, createTime: String, logs: DataTable): Unit = {
        val writer = FileWriter(s"""${Global.QROSS_HOME}tasks/$jobId/${createTime.replace("-", "").substring(0, 8)}/$taskId.log""")

        logs.foreach(log => {
            writer.writeLine(
                Json.serialize(
                    TaskLogPlain(
                        log.getString("record_time"),
                        log.getInt("command_id"),
                        log.getLong("action_id"),
                        log.getString("log_type"),
                        log.getString("log_text"),
                        log.getString("create_time")
                    )
                )
            )
        })

        writer.close()
        logs.clear()
    }

    //restore logs to database
    def restore(jobId: Int, taskId: Long, createTime: String): Unit = {

        val file = new File(s"""${Global.QROSS_HOME}tasks/$jobId/${createTime.replace("-", "").substring(0, 8)}/$taskId.log""")

        val ds = DataSource.QROSS

        FileReader(file)
            .asJsonLines
            .readAsTable(table => {
                ds.tableInsert(s"INSERT INTO qross_tasks_logs (job_id, task_id, record_time, command_id, action_id, log_type, log_text, create_time) VALUES ($jobId, $taskId, '#recordTime', #commandId, #actionId, '#logType', '#logText', '#createTime')", table)
            }).close()

        ds.executeNonQuery(s"UPDATE qross_tasks SET saved='no' WHERE id=$taskId")

        ds.close()

        file.delete()
    }

    def remove(jobId: Int, taskId: Long, createTime: String): Unit = {

    }
}

case class TaskLogPlain (
                            recordTime: String = "",
                            commandId: Int = 0,
                            actionId: Long = 0L,
                            logType: String = "",
                            logText: String = "",
                            createTime: String = ""
                        )
