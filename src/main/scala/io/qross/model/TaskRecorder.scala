package io.qross.model

import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue}

import io.qross.core.DataTable
import io.qross.ext.Output
import io.qross.ext.TypeExt._
import io.qross.fs.FileWriter
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
            val writers = new mutable.HashMap[String, FileWriter]()

            var i = 0
            while(TaskRecorder.LOGS.size() > 0 && i < 10000) {
                val log = TaskRecorder.LOGS.poll()
                if (!writers.contains(log.path)) {
                    writers += log.path -> FileWriter(s"${Global.QROSS_HOME}/tasks/${log.path}.log")
                }
                writers(log.path).writeJsonLine[TaskLogLine](log.line)
                i += 1
            }

            writers.values.foreach(_.close())
        }
    }

    def toHTML(logs: DataTable): String = {
        val sb = new StringBuilder()
        logs.foreach(row => {
            var time = row.getString("create_time")
            if (time.contains(".")) {
                time = time.substring(0, time.indexOf("."))
            }

            sb.append("<div class='TIME'>")
            sb.append(time)
            sb.append("</div>")
            sb.append("<div class='" + row.getString("log_type") + "'>")
            sb.append(row.getString("log_text").replace("\r", "<br/>").replace("\t", "&nbsp;&nbsp;&nbsp;&nbsp;"))
            sb.append("</div>")
        })
        sb.toString()
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
        TaskRecorder.LOGS.add(new TaskLog(jobId, taskId, recordTime, new TaskLogLine(commandId, actionId, seal, text)))
        if (TaskRecorder.LOGS.size() >= 2000) {
            TaskRecorder.save()
        }
        this
    }

    def dispose(): Unit = {
        TaskRecorder.dispose(taskId)
    }
}

class TaskLog(var jobId: Int, var taskId: Long, var recordTime: String, val line: TaskLogLine) {
    val path: String = s"""${recordTime.takeBefore(" ").replace("-", "")}/$jobId/${taskId}_${recordTime.replace("-", "").replace(" ", "").replace(":", "")}"""
}

class TaskLogLine(var commandId: Int = 0, var actionId: Long = 0L, var logType: String = "INFO", var logText: String = "", var logTime: String = DateTime.now.getString("yyyy-MM-dd HH:mm:ss")) {
    if (logType != "INFO" && logType != "ERROR") {
        logText = s"$logTime [$logType] $logText"
    }
}