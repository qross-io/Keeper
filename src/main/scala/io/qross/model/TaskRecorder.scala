package io.qross.model

import java.util.concurrent.ConcurrentHashMap

import io.qross.util.{DataSource, DataTable, DateTime, Output}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object TaskRecorder {

    // Map<taskId, new TaskRecord>
    //val loggers = new mutable.HashMap[Long, TaskRecord]()
    val loggers = new ConcurrentHashMap[Long, TaskRecorder]()

    def of(jobId: Int, taskId: Long, recordTime: String): TaskRecorder = {
        if (!loggers.contains(taskId)) {
            //loggers += (taskId -> new TaskRecord(jobId, taskId))
            loggers.put(taskId, new TaskRecorder(jobId, taskId, recordTime))
        }
         loggers.get(taskId)
    }

    def dispose(taskId: Long): Unit = {
        if (loggers.contains(taskId)) {
            loggers.get(taskId).save()
            loggers.remove(taskId)
        }
    }

    def saveAll(): Unit = {
        loggers.keySet.forEach(taskId => {
            if (loggers.contains(taskId) && loggers.get(taskId).overtime) {
                loggers.get(taskId).save()
            }
        })
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

    //private val logs = new ArrayBuffer[TaskLog]()
    private val logs = new java.util.concurrent.ConcurrentLinkedQueue[TaskLog]()

    private var tick = System.currentTimeMillis()

    def overtime: Boolean = System.currentTimeMillis() - tick > 1500

    def run(commandId: Int, actionId: Long): TaskRecorder = {
        this.commandId = commandId
        this.actionId = actionId
        this
    }

    //记录被调度任务运行过程中的输出流
    def out(info: String): Unit = {
        record("INFO", info)
    }

    //记录被调度任务运行过程中的错误流
    def err(error: String): Unit = {
        record("ERROR", error)
    }

    //记录被调度任务在执行前后的信息
    def log(info: String): Unit = {
        record("LOG", info)
    }

    //记录被调度任务在执行前后的警告信息，会被记录成系统日志
    def warn(warning: String): Unit = {
        Output.writeWarning(warning)
        record("WARN", warning)
    }

    //记录被调度任务在执行前后的重要信息，会被记录成系统日志
    def debug(message: String): Unit = {
        Output.writeDebugging(message)
        record("DEBUG", message)
    }
    
    private def record(seal: String, text: String): Unit = {
        logs.add(new TaskLog(jobId, taskId, recordTime, commandId, actionId, seal, text))
        if (overtime) {
            save()
        }
    }
    
    def save(): Unit = synchronized {
        if (logs.size() > 0) {
            val ds = new DataSource()
            ds.setBatchCommand(s"INSERT INTO qross_tasks_logs (job_id, task_id, record_time, command_id, action_id, log_type, log_text, create_time) VALUES (?, ?, ?, ?, ?, ?, ?, ?)")
            while(logs.size() > 0) {
                val log = logs.poll()
                ds.addBatch(log.jobId, log.taskId, log.recordTime, log.commandId, log.actionId, log.logType, log.logText, log.logTime)
            }
            ds.executeBatchUpdate()
            ds.close()
        }
        tick = System.currentTimeMillis()
    }
}

class TaskLog(var jobId: Int, var taskId: Long, var recordTime: String, var commandId: Int = 0, var actionId: Long = 0L, var logType: String = "INFO", var logText: String = "", var logTime: String = DateTime.now.getString("yyyy-MM-dd HH:mm:ss")) {
    
    private val limit = 65535
    
    if (logType != "INFO" && logType != "ERROR") {
        logText = s"$logTime [$logType] $logText"
    }
    if (logText.length > limit) logText = logText.take(limit)

    override def toString: String = {
        s"Job: $jobId, Task: $taskId @ $recordTime, Command: $commandId, Action: $actionId - $logTime [$logType] ${logText.replace("\r", "\\r")}"
    }
}