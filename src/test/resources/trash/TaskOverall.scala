import com.fasterxml.jackson.databind.ObjectMapper
import io.qross.model.{Global, TaskRecordPlain}
import io.qross.util.{DataRow, DataTable, DateTime, FileWriter}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class TaskOverall(row: DataRow) {

    val taskId: Long = row.getLong("id")
    val jobId: Int = row.getInt("job_id")
    val taskTime: String = row.getString("task_time")
    val recordTime: String = row.getString("recordTime")
    val creator: String = row.getString("creator")
    val createMode: String = row.getString("create_mode")
    val startMode: String = row.getString("start_mode")
    val status: String = row.getString("status")
    val startTime: String = row.getString("start_time")
    val finishTime: String = row.getString("finish_time")
    val spent: Int = row.getInt("spent")
    val createTime: String = row.getString("create_time")
    val updateTime: String = row.getString("update_time")

    val records = new mutable.LinkedHashMap[String, TaskRecordPlain]()

    def addRecords(table: DataTable): TaskOverall = {
        table.foreach(row => {
            records += row.getString("record_time") -> TaskRecordPlain().fill(row)
        })
        this
    }

    def addActions(table: DataTable): TaskOverall = {
        for ((time, record) <- records) {
            table.filter(row => row.getString("record_time") == time)
                    .foreach(row => {
                        record.addAction(row)
                    })
        }
        this
    }

    def addDepends(table: DataTable): TaskOverall = {
        for ((time, record) <- records) {
            table.filter(row => row.getString("record_time") == time)
                    .foreach(row => {
                        record.addDependency(row)
                    })
        }
        this
    }

    def addLogs(table: DataTable): TaskOverall = {
        for ((time, record) <- records) {
            table.filter(row => row.getString("record_time") == time)
                    .foreach(row => {
                        record.addLog(row)
                    })
        }
        this
    }

    def store(): Unit = {
        val writer = FileWriter(s"""${Global.QROSS_HOME}keeper/tasks/$jobId/${if (taskTime.length >= 8) taskTime.substring(0, 8) else DateTime.now.getString("yyyyMMdd") }/$taskId.json""")
        writer.writeLine(new ObjectMapper().writeValueAsString(this))
        writer.close()
    }
}

class OverallRecord(row: DataRow) {

    val recordTime: String = row.getString("recordTime")
    val creator: String = row.getString("creator")
    val createMode: String = row.getString("create_mode")
    val startMode: String = row.getString("start_mode")
    val status: String = row.getString("status")
    val startTime: String = row.getString("start_time")
    val finishTime: String = row.getString("finish_time")
    val spent: Int = row.getInt("spent")
    val createTime: String = row.getString("create_time")

    var actions = new ArrayBuffer[OverallAction]()
    var dependencies = new ArrayBuffer[OverallDependency]()
    var logs = new ArrayBuffer[OverallLog]()

    def addAction(row: DataRow): Unit = {
        actions += new OverallAction(row)
    }

    def addDependency(row: DataRow): Unit = {
        dependencies += new OverallDependency(row)
    }

    def addLog(row: DataRow): Unit = {
        logs += new OverallLog(row)
    }
}

class OverallAction (row: DataRow) {
    val actionId: Long = row.getLong("id")
    val commandId: Int = row.getInt("command_id")
    val upstreamIds: String = row.getString("upstream_ids")
    val commandText: String = row.getString("command_text")
    val status: String = row.getString("status")
    val startTime: String = row.getString("start_time")
    val runTime: String = row.getString("run_time")
    val finishTime: String = row.getString("finish_time")
    val elapsed: Int = row.getInt("elapsed")
    val waiting: Int = row.getInt("waiting")
    val retryTimes: Int = row.getInt("retry_times")
    val createTime: String =row.getString("create_time")
    val updateTime: String =row.getString("update_time")
}

class OverallDependency (row: DataRow) {
    val dependencyId: Int = row.getInt("dependency_id")
    val dependencyMoment: String = row.getString("dependency_moment")
    val dependencyType: String = row.getString("dependency_type")
    val dependencyValue: String = row.getString("dependency_value")
    val ready: String = row.getString("ready")
    val retryTimes: Int = row.getInt("retry_times")
    val createTime: String = row.getString("create_time")
    val updateTime: String = row.getString("update_time")
}

class OverallLog (row: DataRow) {
    val commandId: Int = row.getInt("command_id")
    val actionId: Long = row.getLong("action_id")
    val logType: String = row.getString("log_type")
    val logText: String = row.getString("log_text")
    val createTime: String = row.getString("create_time")
}