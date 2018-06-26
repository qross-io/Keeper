package io.qross.model

import java.util.concurrent.ConcurrentHashMap

import io.qross.util.{DataSource, DateTime, Output}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object TaskRecord {
    
    // Map<taskId, new TaskRecord>
    val loggers = new mutable.HashMap[Long, TaskRecord]()
    
    def of(jobId: Int, taskId: Long): TaskRecord = {
        if (!loggers.contains(taskId)) {
            loggers += (taskId -> new TaskRecord(jobId, taskId))
        }
        loggers(taskId)
    }
    
    def dispose(taskId: Long): Unit = {
        if (loggers.contains(taskId)) {
            loggers(taskId).save()
            loggers -= taskId
        }
    }
    
    def saveAll(): Unit = {
        loggers.keySet.foreach(taskId => {
            if (loggers.contains(taskId) && loggers(taskId).overtime) {
                loggers(taskId).save()
            }
        })
    }
}

class TaskRecord(jobId: Int, taskId: Long) {
    
    var commandId = 0
    var actionId = 0L
    
    private val logs = new ArrayBuffer[TaskLog]()
    private var tick = System.currentTimeMillis()
    
    def overtime: Boolean = System.currentTimeMillis() - tick > 2000
    
    def run(commandId: Int, actionId: Long): TaskRecord = {
        this.commandId = commandId
        this.actionId = actionId
        this
    }
    
    def out(info: String): Unit = {
        record("INFO", info)
    }
    
    def err(error: String): Unit = {
        record("ERROR", error)
    }
    
    def log(info: String): Unit = {
        record("LOG", info)
    }
    
    def warn(warning: String): Unit = {
        record("WARN", warning)
    }
    
    def debug(message: String): Unit = {
        record("DEBUG", message)
    }
    
    private def record(seal: String, text: String): Unit = {
        logs += new TaskLog(jobId, taskId, commandId, actionId, seal, text)
        if (overtime) {
            save()
        }
    }
    
    def save(): Unit = synchronized {
        if (logs.nonEmpty) {
            val log = new TaskLog(0, 0)
            val ds = new DataSource()
            ds.setBatchCommand(s"INSERT INTO qross_tasks_logs (job_id, task_id, command_id, action_id, log_type, log_text, create_time) VALUES (?, ?, ?, ?, ?, ?, ?)")
            logs.foreach(line => {
                if (!log.merge(line)) {
                    ds.addBatch(log.jobId, log.taskId, log.commandId, log.actionId, log.logType, log.logText, log.logTime)
                    log.copy(line)
                }
            })
            if (log.taskId > 0) {
                ds.addBatch(log.jobId, log.taskId, log.commandId, log.actionId, log.logType, log.logText, log.logTime)
            }
            ds.executeBatchUpdate()
            ds.close()
            
            logs.clear()
        }
        tick = System.currentTimeMillis()
    }
}

class TaskLog(var jobId: Int, var taskId: Long, var commandId: Int = 0, var actionId: Long = 0L, var logType: String = "INFO", var logText: String = "", var logTime: String = DateTime.now.getString("yyyy-MM-dd HH:mm:ss")) {
    
    if (logType != "INFO" && logType != "ERROR") {
        logText = s"$logTime [$logType] $logText"
        logType = "INFO"
    }
    if (logText.length > 65535) logText = logText.take(65535)
    
    
    def merge(line: TaskLog): Boolean = {
        var merged = false
        if (this.taskId == 0) {
            this.copy(line)
            merged = true
        }
        else if (this.taskId == line.taskId && this.actionId == line.actionId && this.logType == line.logType && this.logTime == line.logTime) {
            val text = this.logText + "\r" + line.logText
            if (text.length < 65535) {
                this.logText = text
                merged = true
            }
        }
        merged
    }
    
    def copy(line: TaskLog): Unit = {
        this.taskId = line.taskId
        this.jobId = line.jobId
        this.commandId = line.commandId
        this.actionId = line.actionId
        this.logType = line.logType
        this.logText = line.logText
        this.logTime = line.logTime
    }
    
    override def toString: String = {
        s"Job: $jobId, Task: $taskId, Command: $commandId, Action: $actionId - $logTime [$logType] ${logText.replace("\r", "\\r")}"
    }
}