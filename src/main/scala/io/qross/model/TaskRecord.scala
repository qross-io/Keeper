package io.qross.model

import java.util.concurrent.ConcurrentHashMap

import io.qross.util.{DataSource, DataTable, DateTime, Output}

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
        Output.writeWarning(warning)
        record("WARN", warning)
    }
    
    def debug(message: String): Unit = {
        Output.writeDebugging(message)
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
            try {
                ds.executeBatchUpdate()
            }
            catch {
                case e: Exception => e.printStackTrace()
            }
            ds.close()
            
            logs.clear()
        }
        tick = System.currentTimeMillis()
    }
}

class TaskLog(var jobId: Int, var taskId: Long, var commandId: Int = 0, var actionId: Long = 0L, var logType: String = "INFO", var logText: String = "", var logTime: String = DateTime.now.getString("yyyy-MM-dd HH:mm:ss")) {
    
    private val limit = 65535
    
    if (logType != "INFO" && logType != "ERROR") {
        logText = s"$logTime [$logType] $logText"
        logType = "INFO"
    }
    if (logText.length > limit) logText = logText.take(limit)
    /*
    if (logText.getBytes().length > limit) {
        var i = 1
        do {
            logText = logText.take(logText.length - i)
            i += 1
        }
        while(logText.getBytes().length > limit)
    } */
    
    /*
    MySQL数据库中存储数据长度的坑
    如果用实际文本长度，在数据库中的长度会远大于实际文本长度，大约是 65535比170003
    所以会造成在使用text类型时存储出错，长度过长
    改正方法1是将text改为mediumtext
    改正方法2是转化成字节getBytes()再计算长度
    * */
    
    def merge(line: TaskLog): Boolean = {
        var merged = false
        if (this.taskId == 0) {
            this.copy(line)
            merged = true
        }
        else if (this.taskId == line.taskId && this.actionId == line.actionId && this.logType == line.logType && this.logTime == line.logTime) {
            val text = this.logText + "\r" + line.logText
            if (text.length < limit) { //.getBytes()
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