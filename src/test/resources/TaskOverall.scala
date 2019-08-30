package io.qross.model

import io.qross.core.{DataRow, DataTable}
import io.qross.fs.FileWriter
import io.qross.jdbc.DataSource
import io.qross.net.Json
import io.qross.setting.Global
import io.qross.time.DateTime

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object TaskOverall {

    def of(taskId: Long): TaskOverall = {

        val ds = new DataSource()
        val task = ds.executeDataRow(s"SELECT id, job_id, task_time, status, create_time, update_time FROM qross_tasks WHERE id=$taskId")
        val records = ds.executeDataTable(s"""SELECT record_time, status, start_mode, start_time, finish_time, spent, 'N/A' AS store_time FROM qross_tasks WHERE id=$taskId
                                           UNION ALL SELECT record_time, status, start_mode, start_time, finish_time, spent, create_time AS store_time FROM qross_tasks_records WHERE task_id=$taskId ORDER BY record_time DESC""")
        val actions = ds.executeDataTable(s"SELECT * FROM qross_tasks_dags WHERE task_id=$taskId")
        val depends = ds.executeDataTable(s"SELECT * FROM qross_tasks_dependencies WHERE task_id=$taskId")
        val logs = ds.executeDataTable(s"SELECT * FROM qross_tasks_logs WHERE task_id=$taskId")
        val events = ds.executeDataTable(s"SELECT * FROM qross_tasks_events WHERE task_id=$taskId")
        ds.close()

        null
        //TaskOverall().fill(task).addRecords(records).addActions(actions).addDepends(depends).addLogs(logs).addEvents(events)
    }

    /*
    def store(): Unit = {
        val writer = FileWriter(s"""${Global.QROSS_KEEPER_HOME}tasks/$jobId/${if (taskTime.length >= 8) taskTime.substring(0, 8) else DateTime.now.getString("yyyyMMdd") }/$taskId.json""")
        writer.writeLine(Json.serialize(this))
        //Output.writeLine(Json.toString(this))
        writer.close()
    }
    */

    def save(taskId: Long, recordTime: String): Unit = {
        
    }
}

case class TaskOverall(
          taskId: Long = 0L,
          jobId: Int = 0,
          taskTime: String = "",
          creator: String = "",
          createMode: String = "",
          createTime: String = "",
          updateTime: String = "",
          status: String = "",
          recordTime: String = "",
          startMode: String = "",
          startTime: String = "",
          finishTime: String = "",
          spent: Int = 0,
          storeTime: String = "",
          events: ArrayBuffer[TaskEventPlain] = new ArrayBuffer[TaskEventPlain](),
          actions: ArrayBuffer[TaskActionPlain] = new ArrayBuffer[TaskActionPlain](),
          dependencies: ArrayBuffer[TaskDependencyPlain] = new ArrayBuffer[TaskDependencyPlain](),
          logs: ArrayBuffer[TaskLogPlain] = new ArrayBuffer[TaskLogPlain]()
    )

case class TaskOverall (
        var taskId: Long = 0L,
        var jobId: Int = 0,
        var taskTime: String = "",
        var creator: String = "",
        var createMode: String = "",
        var createTime: String = "",
        var updateTime: String = "",

        records: mutable.LinkedHashMap[String, TaskRecordPlain] = new mutable.LinkedHashMap[String, TaskRecordPlain]()
   ) {

    def fill(row: DataRow): TaskOverall = {
        taskId = row.getLong("id")
        jobId = row.getInt("job_id")
        taskTime = row.getString("task_time")
        creator = row.getString("creator")
        createMode = row.getString("create_mode")
        createTime = row.getString("create_time")
        updateTime = row.getString("update_time")

        this
    }


    def addRecords(table: DataTable): TaskOverall = {
        table.foreach(row => {
            records += row.getString("record_time") -> TaskRecordPlain().fill(row)
        })
        this
    }

    def addActions(table: DataTable): TaskOverall = {
        table.foreach(row => {
            val time = row.getString("record_time")
            if (records.contains(time)) {
                records(time).addAction(row)
            }
        })
        this
    }

    def addDepends(table: DataTable): TaskOverall = {
        table.foreach(row => {
            val time = row.getString("record_time")
            if (records.contains(time)) {
                records(time).addDependency(row)
            }
        })
        this
    }

    def addLogs(table: DataTable): TaskOverall = {
        table.foreach(row => {
            val time = row.getString("record_time")
            if (records.contains(time)) {
                records(time).addLog(row)
            }
        })
        this
    }

    def addEvents(table: DataTable): TaskOverall = {
        table.foreach(row => {
            val time = row.getString("record_time")
            if (records.contains(time)) {
                records(time).addEvent(row)
            }
        })
        this
    }
}

case class TaskRecordLogs(
                 var recordTime: String = "",
                 logs: ArrayBuffer[TaskLogPlain] = new ArrayBuffer[TaskLogPlain]()
             )

case class TaskRecordPlain (
        var recordTime: String = "",
        var status: String = "",
        var startMode: String = "",
        var startTime: String = "",
        var finishTime: String = "",
        var spent: Int = 0,
        var storeTime: String = "",
        events: ArrayBuffer[TaskEventPlain] = new ArrayBuffer[TaskEventPlain](),
        actions: ArrayBuffer[TaskActionPlain] = new ArrayBuffer[TaskActionPlain](),
        dependencies: ArrayBuffer[TaskDependencyPlain] = new ArrayBuffer[TaskDependencyPlain](),
        logs: ArrayBuffer[TaskLogPlain] = new ArrayBuffer[TaskLogPlain]()
    ) {

    def fill(row: DataRow): TaskRecordPlain = {
        recordTime = row.getString("record_time")
        status = row.getString("status")
        startMode = row.getString("start_mode")
        startTime = row.getString("start_time")
        finishTime = row.getString("finish_time")
        spent = row.getInt("spent")
        storeTime = row.getString("store_time")

        this
    }

    def addAction(row: DataRow): Unit = {
        actions += TaskActionPlain().fill(row)
    }

    def addDependency(row: DataRow): Unit = {
        dependencies += TaskDependencyPlain().fill(row)
    }

    def addLog(row: DataRow): Unit = {
        logs += TaskLogPlain().fill(row)
    }

    def addEvent(row: DataRow): Unit = {
        events += TaskEventPlain().fill(row)
    }
}

case class TaskEventPlain (
        var eventName: String = "",
        var eventFunction: String = "",
        var eventValue: String = "",
        var eventOption: Int = 0,
        var createTime: String = "",
        var updateTime: String = ""
    ) {

    def fill(row: DataRow): TaskEventPlain = {
        eventName = row.getString("event_name")
        eventFunction = row.getString("event_function")
        eventValue = row.getString("event_value")
        eventOption = row.getInt("event_option")
        createTime = row.getString("create_time")
        updateTime = row.getString("update_time")

        this
    }
}

case class TaskActionPlain (
        var actionId: Long = 0L,
        var commandId: Int = 0,
        var upstreamIds: String = "",
        var commandText: String = "",
        var status: String = "",
        var startTime: String = "",
        var runTime: String = "",
        var finishTime: String = "",
        var elapsed: Int = 0,
        var waiting: Int = 0,
        var retryTimes: Int = 0,
        var createTime: String = "",
        var updateTime: String = ""
      ) {

    def fill(row: DataRow): TaskActionPlain = {
        actionId = row.getLong("id")
        commandId = row.getInt("command_id")
        upstreamIds = row.getString("upstream_ids")
        commandText = row.getString("command_text")
        status = row.getString("status")
        startTime = row.getString("start_time")
        runTime = row.getString("run_time")
        finishTime = row.getString("finish_time")
        elapsed = row.getInt("elapsed")
        waiting = row.getInt("waiting")
        retryTimes = row.getInt("retry_times")
        createTime = row.getString("create_time")
        updateTime = row.getString("update_time")

        this
    }
}

case class TaskDependencyPlain (
        var dependencyId: Int = 0,
        var dependencyMoment: String = "",
        var dependencyType: String = "",
        var dependencyValue: String = "",
        var ready: String = "",
        var retryTimes: Int = 0,
        var createTime: String = "",
        var updateTime: String = ""
      ) {

    def fill(row: DataRow): TaskDependencyPlain = {
        dependencyId = row.getInt("dependency_id")
        dependencyMoment = row.getString("dependency_moment")
        dependencyType = row.getString("dependency_type")
        dependencyValue = row.getString("dependency_value")
        ready = row.getString("ready")
        retryTimes = row.getInt("retry_times")
        createTime = row.getString("create_time")
        updateTime = row.getString("update_time")

        this
    }
}

case class TaskLogPlain (
               var commandId: Int = 0,
               var actionId: Long = 0L,
               var logType: String = "",
               var logText: String = "",
               var createTime: String = ""
           ) {

    def fill(row: DataRow): TaskLogPlain = {
        commandId = row.getInt("command_id")
        actionId = row.getLong("action_id")
        logType = row.getString("log_type")
        logText = row.getString("log_text")
        createTime = row.getString("create_time")

        this
    }
}
