package io.qross.model

import com.fasterxml.jackson.databind.ObjectMapper
import io.qross.util._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object TaskOverall {

    def of(taskId: Long): TaskOverall = {

        val ds = new DataSource()
        val task = ds.executeDataRow(s"SELECT * FROM qross_tasks WHERE id=$taskId")
        val records = ds.executeDataTable(s"SELECT * FROM qross_tasks_records WHERE task_id=$taskId")
        val actions = ds.executeDataTable(s"SELECT * FROM qross_tasks_dags WHERE task_id=$taskId")
        val depends = ds.executeDataTable(s"SELECT * FROM qross_tasks_dependencies WHERE task_id=$taskId")
        val logs = ds.executeDataTable(s"SELECT * FROM qross_task_logs WHERE task_id=$taskId")
        ds.close()

        new TaskOverall(task).addRecords(records).addActions(actions).addDepends(depends).addLogs(logs)
    }
}

class TaskOverall(row: DataRow) {

    val taskId = row.getLong("id")
    val jobId = row.getInt("job_id")
    val taskTime = row.getString("task_time")
    val recordTime = row.getString("recordTime")
    val creator = row.getString("creator")
    val createMode = row.getString("create_mode")
    val startMode = row.getString("start_mode")
    val status = row.getString("status")
    val startTime = row.getString("start_time")
    val finishTime = row.getString("finish_time")
    val spent = row.getInt("spent")
    val createTime = row.getString("create_time")
    val updateTime = row.getString("update_time")

    val records = new mutable.LinkedHashMap[String, OverallRecord]()

    def addRecords(table: DataTable): TaskOverall = {
        table.foreach(row => {
            records += row.getString("record_time") -> new OverallRecord(row)
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
        val writer = new FileWriter(s"""${Global.QROSS_HOME}keeper/tasks/$jobId/${if (taskTime.length >= 8) taskTime.substring(0, 8) else DateTime.now.getString("yyyyMMdd") }/$taskId.json""")
        writer.writeLine(new ObjectMapper().writeValueAsString(this))
        writer.close()
    }
}

class OverallRecord(row: DataRow) {

    var recordTime = row.getString("recordTime")
    var creator = row.getString("creator")
    var createMode = row.getString("create_mode")
    var startMode = row.getString("start_mode")
    val status = row.getString("status")
    val startTime = row.getString("start_time")
    val finishTime = row.getString("finish_time")
    val spent = row.getInt("spent")
    val createTime = row.getString("create_time")

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
    val actionId = row.getLong("id")
    val commandId = row.getInt("command_id")
    val upstreamIds = row.getString("upstream_ids")
    val commandText = row.getString("command_text")
    val status = row.getString("status")
    val startTime = row.getString("start_time")
    val runTime = row.getString("run_time")
    val finishTime = row.getString("finish_time")
    val elapsed = row.getInt("elapsed")
    val waiting = row.getInt("waiting")
    val retryTimes = row.getInt("retry_times")
    val createTime =row.getString("create_time")
    val updateTime =row.getString("update_time")
}

class OverallDependency (row: DataRow) {
    val dependencyId = row.getInt("dependency_id")
    val dependencyMoment = row.getString("dependency_moment")
    val dependencyType = row.getString("dependency_type")
    val dependencyValue = row.getString("dependency_value")
    val ready = row.getString("ready")
    val retryTimes = row.getInt("retry_times")
    val createTime = row.getString("create_time")
    val updateTime = row.getString("update_time")
}

class OverallLog (row: DataRow) {
    val commandId = row.getInt("command_id")
    val actionId = row.getLong("action_id")
    val logType = row.getString("log_type")
    val logText = row.getString("log_text")
    val createTime = row.getString("create_time")
}