package io.qross.model

import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue}

import io.qross.core.DataTable
import io.qross.ext.Output
import io.qross.ext.TypeExt._
import io.qross.fs.FileWriter
import io.qross.jdbc.DataSource
import io.qross.setting.Global
import io.qross.time.DateTime

import scala.collection.mutable

object TaskRecorder {

    private val loggers = new ConcurrentHashMap[(Long, String), TaskRecorder]()
    private val LOGS = new ConcurrentLinkedQueue[TaskLog]()

    def of(jobId: Int, taskId: Long, recordTime: String): TaskRecorder = {
        if (!loggers.containsKey((taskId, recordTime))) {
            loggers.put((taskId, recordTime), new TaskRecorder(jobId, taskId, recordTime))
        }
        loggers.get((taskId, recordTime))
    }

    def dispose(taskId: Long): Unit = {
        if (loggers.contains(taskId)) {
            loggers.remove(taskId)
        }
    }

    def save(): Unit = synchronized {

        //qross_home/tasks/record_time_day/jobId/task_id_record_time.log

        if (TaskRecorder.LOGS.size() > 0) {

            //val ds = DataSource.QROSS
            val writers = new mutable.HashMap[String, FileWriter]()

            var i = 0 //每次最多保存10000条数据
            //ds.setBatchCommand(s"INSERT INTO qross_tasks_logs (job_id, task_id, record_time, command_id, action_id, log_type, log_text, create_time) VALUES (?, ?, ?, ?, ?, ?, ?, ?)")
            while(TaskRecorder.LOGS.size() > 0 && i < 10000) {
                val log = TaskRecorder.LOGS.poll()
                if (!writers.contains(log.path)) {
                    writers += log.path -> new FileWriter(s"${Global.QROSS_HOME}/tasks/${log.path}.log")
                }

                //ds.addBatch(log.jobId, log.taskId, log.recordTime, log.line.commandId, log.line.actionId, log.line.logType, log.line.logText, log.line.logTime)
                writers(log.path).writeObjectLine(log.line)

                i += 1
            }

            //ds.executeBatchUpdate()
            //ds.close()
            writers.values.foreach(_.close())
        }
    }
}

class TaskRecorder(jobId: Int, taskId: Long, recordTime: String) {

    var commandId = 0
    var actionId = 0L

    def run(commandId: Int, actionId: Long): TaskRecorder = {
        this.commandId = commandId
        this.actionId = actionId
        this
    }

    //记录被调度任务运行过程中的输出流
    def out(info: String): TaskRecorder = {
        record("INFO", info)
    }

    //记录被调度任务运行过程中的错误流
    def err(error: String): TaskRecorder = {
        record("ERROR", error)
    }

    //记录被调度任务在执行前后的信息
    def log(info: String): TaskRecorder = {
        Output.writeMessage(info)
        record("LOG", info)
    }

    //记录被调度任务在执行前后的警告信息，会被记录成系统日志
    def warn(warning: String): TaskRecorder = {
        Output.writeWarning(warning)
        record("WARN", warning)
    }

    //记录被调度任务在执行前后的重要信息，会被记录成系统日志
    def debug(message: String): TaskRecorder = {
        Output.writeDebugging(message)
        record("DEBUG", message)
    }

    private def record(seal: String, text: String): TaskRecorder = {
        TaskRecorder.LOGS.add(
            new TaskLog(jobId, taskId, recordTime,
                new TaskLogLine(commandId, actionId, seal, text)
            )
        )
        if (TaskRecorder.LOGS.size() >= 2000) {
            TaskRecorder.save()
        }
        this
    }

    def dispose(): Unit = {
        TaskRecorder.dispose(taskId)
    }
}

//完整的一行日志
class TaskLog(val jobId: Int, val taskId: Long, val recordTime: String, val line: TaskLogLine) {
    val path: String = s"""${recordTime.takeBefore(" ").replace("-", "")}/$jobId/${taskId}_${recordTime.takeAfter(" ").replace(":", "")}"""
}

//保存在日志文件中的每一行
class TaskLogLine(val commandId: Int, val actionId: Long, val logType: String = "INFO", var logText: String = "", val logTime: String = DateTime.now.getString("yyyy-MM-dd HH:mm:ss")) {
    if (logType != "INFO" && logType != "ERROR") {
        logText = s"$logTime [$logType] $logText"
    }
}