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
    
    
    def main(args: Array[String]): Unit = {
        val logger = TaskRecord.of(1, 1).run(0, 0)
        for (i <- 0 until 10000) {
            logger.err(
                i + """ :
                 |    今年Q1季度是全球矿卡销售的巅峰，即将过去的Q2季度中矿卡市场就崩了，矿卡销量直线下滑，这也导致了全球显卡库存大增，业界消息称目前全球显卡库存还有数百万，这也是导致AMD、NVIDIA不愿意推新卡的一个重要原因，而7月份显卡只能继续降价。
                 |全球加密货币市场的寒潮导致矿卡需求急剧下降，digitimies援引业界消息称供应商正准备降价以清空库存。根据他们的爆料，7月份显卡可能还会继续降价20%。
                 |此外，矿卡市场的萎缩也会影响ASIC芯片订单，所以台积电、Global Unichip等芯片设计、制造公司2018年的业绩也会受到影响，一些中小型矿场逐渐退出市场，大型矿场也削减了新矿机的采购量。
                 |那么具体的显卡库存到底有多大呢？消息人士的话称全球显卡的库存量高达数百万，而NVIDIA新一代显卡发布的备货量也不低，原文说有一百万GPU芯片待发——听着实在有点夸张了点。
                 |此外，除了库存显卡积压，原文还提到工头们也准备把大量矿卡卖到零售渠道中，供应商预计显卡还会大降价。有意思的是，他们的最后一条爆料竟然提到了NVIDIA的7nm新卡，表示由于库存过高，采用台积电12nm及7nm工艺的GPU将推迟到2018年Q4季度末。""".stripMargin)
        }
        TaskRecord.dispose(1)
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
    if (logText.getBytes().length > limit) {
        var i = 1
        do {
            logText = logText.take(logText.length - i)
            i += 1
        }
        while(logText.getBytes().length > limit)
    }
    
    
    def merge(line: TaskLog): Boolean = {
        var merged = false
        if (this.taskId == 0) {
            this.copy(line)
            merged = true
        }
        else if (this.taskId == line.taskId && this.actionId == line.actionId && this.logType == line.logType && this.logTime == line.logTime) {
            val text = this.logText + "\r" + line.logText
            if (text.getBytes().length < limit) {
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