package io.qross.model

import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue}

import io.qross.ext.Output
import io.qross.ext.TypeExt._
import io.qross.fs.FileWriter
import io.qross.setting.Global
import io.qross.time.DateTime

object TaskRecorder {

    val loggers = new ConcurrentHashMap[String, TaskRecorder]()

    def of(jobId: Int, taskId: Long, recordTime: String): TaskRecorder = {
        val key = taskId + "@" + recordTime
        if (!loggers.containsKey(key)) {
            loggers.put(key, new TaskRecorder(jobId, taskId, recordTime))
        }
        loggers.get(key)
    }

    def save(): Unit = synchronized {
        loggers.values.forEach(_.save())
    }

    def dispose(): Unit = synchronized {
        loggers.values.forEach(_.dispose())
    }
}

class TaskRecorder(jobId: Int, taskId: Long, recordTime: String) {
    //qross_home/tasks/record_time_day/jobId/task_id_record_time.log
    private val path = s"""${Global.QROSS_HOME}tasks/${recordTime.takeBefore(" ").replace("-", "")}/$jobId/${taskId}_${recordTime.takeAfter(" ").replace(":", "")}.log"""
    private val logs = new ConcurrentLinkedQueue[TaskLogLine]()

    //记录被调度任务运行过程中的输出流
    def out(info: String, commandId: Int = 0, actionId: Long = 0): TaskRecorder = {
        record("INFO", info, commandId, actionId)
    }

    //记录被调度任务运行过程中的错误流
    def err(error: String, commandId: Int = 0, actionId: Long = 0): TaskRecorder = {
        record("ERROR", error, commandId, actionId)
    }

    //记录被调度任务在执行前后的信息
    def log(info: String, commandId: Int = 0, actionId: Long = 0): TaskRecorder = {
        Output.writeMessage(info)
        record("LOG", info, commandId, actionId)
    }

    //记录被调度任务在执行前后的警告信息，会被记录成系统日志
    def warn(warning: String, commandId: Int = 0, actionId: Long = 0): TaskRecorder = {
        Output.writeWarning(warning)
        record("WARN", warning, commandId, actionId)
    }

    //记录被调度任务在执行前后的重要信息，会被记录成系统日志
    def debug(message: String, commandId: Int = 0, actionId: Long = 0): TaskRecorder = {
        Output.writeDebugging(message)
        record("DEBUG", message, commandId, actionId)
    }

    private def record(seal: String, text: String, commandId: Int, actionId: Long): TaskRecorder = {
        logs.add(new TaskLogLine(commandId, actionId, seal, {
            if (seal == "INFO" || seal == "ERROR") {
                text
            }
            else {
                s"${DateTime.now} [$seal] $text"
            }
        }))

        this
    }

    def save(): Unit = synchronized {
        if (logs.size() > 0) {
            val writer = new FileWriter(path)
            while (logs.size() > 0) {
                writer.writeObjectLine(logs.poll())
            }
            writer.close()
        }
    }

    def dispose(): Unit = {
        save()
        TaskRecorder.loggers.remove(taskId + "@" + recordTime)
    }
}

//保存在日志文件中的每一行
class TaskLogLine(val commandId: Int, val actionId: Long, val logType: String = "INFO", val logText: String) {

}