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

case class TaskEventPlain (
        var eventName: String = "",
        var eventFunction: String = "",
        var eventValue: String = "",
        var eventOption: Int = 0,
        var createTime: String = "",
        var updateTime: String = ""
    )

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
      )

case class TaskDependencyPlain (
        var dependencyId: Int = 0,
        var dependencyMoment: String = "",
        var dependencyType: String = "",
        var dependencyValue: String = "",
        var ready: String = "",
        var retryTimes: Int = 0,
        var createTime: String = "",
        var updateTime: String = ""
      )

case class TaskLogPlain (
               var commandId: Int = 0,
               var actionId: Long = 0L,
               var logType: String = "",
               var logText: String = "",
               var createTime: String = ""
           )
