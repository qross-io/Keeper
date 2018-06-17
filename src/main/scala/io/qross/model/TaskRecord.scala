package io.qross.model

import java.util.concurrent.ConcurrentHashMap

import io.qross.util.{DataSource, DateTime, Output}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object TaskRecord {
    
    private val logs = new ConcurrentHashMap[Long, ArrayBuffer[TaskLog]]()
    private var count = 0
    private var tick = System.currentTimeMillis()
    
    def +=(log: TaskLog): Unit = {
        if (!logs.contains(log.taskId)) {
            logs.put(log.taskId, new ArrayBuffer[TaskLog]())
        }
        logs.get(log.taskId) += log
        count += 1
    
        if (count >= 200 || System.currentTimeMillis() - tick >= 5000) {
            save()
        }
    }
   
    def save(): Unit = {
        if (count > 0 && !logs.isEmpty) {
            val log = new TaskLog(0L)
            val ds = new DataSource()
            ds.setBatchCommand(s"INSERT INTO qross_tasks_logs (job_id, task_id, command_id, action_id, log_type, log_text, create_time) VALUES (?, ?, ?, ?, ?, ?, ?)")
            logs.keySet().forEach(taskId => {
                for (line <- logs.get(taskId)) {
                    if (!log.merge(line)) {
                        ds.addBatch(log.jobId, taskId, log.commandId, log.actionId, log.logType, log.logText, log.logTime)
                        log.copy(line)
                    }
                }
            })
            if (log.taskId > 0) {
                ds.addBatch(log.jobId, log.taskId, log.commandId, log.actionId, log.logType, log.logText, log.logTime)
            }
            ds.executeBatchUpdate()
            ds.close()
            
            logs.clear()
            tick = System.currentTimeMillis()
            count = 0
        }
    }
}

case class TaskRecord(jobId: Int, taskId: Long, var commandId: Int = 0, var actionId: Long = 0L) {
    
    def run(commandId: Int, actionId: Long): TaskRecord = {
        this.commandId = commandId
        this.actionId = actionId
        this
    }
    
    def out(info: String): Unit = {
        TaskRecord += new TaskLog(taskId).make(jobId, commandId, actionId, "INFO", info)
    }
    
    def err(error: String): Unit = {
        TaskRecord += new TaskLog(taskId).make(jobId, commandId, actionId, "ERROR", error)
    }
    
    def log(info: String): Unit = {
        TaskRecord += new TaskLog(taskId).make(jobId, commandId, actionId, "INFO", DateTime.now.getString("yyyy-MM-dd HH:mm:ss") + " [INFO] " + info)
    }
    
    def warn(warning: String): Unit = {
        TaskRecord += new TaskLog(taskId).make(jobId, commandId, actionId, "INFO", DateTime.now.getString("yyyy-MM-dd HH:mm:ss") + " [WARN] " + warning)
    }
    
    def debug(message: String): Unit = {
        Output.writeDebugging(message)
        TaskRecord += new TaskLog(taskId).make(jobId, commandId, actionId, "INFO", DateTime.now.getString("yyyy-MM-dd HH:mm:ss") + " [DEBUG] " + message)
    }
}

class TaskLog(var taskId: Long) {
    
    var jobId: Int = 0
    var commandId: Int = 0
    var actionId: Long = 0
    var logType: String = ""
    var logText: String = ""
    var logTime: String = ""
    
    def make(jobId: Int, commandId: Int = 0, actionId: Long = 0L, logType: String = "INFO", logText: String = ""): TaskLog = {
        this.jobId = jobId
        this.commandId = commandId
        this.actionId = actionId
        this.logType = logType
        this.logText = if (logText.length < 65535) logText else logText.take(65535)
        this.logTime = DateTime.now.getString("yyyy-MM-dd HH:mm:ss")
        this
    }
    
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
}