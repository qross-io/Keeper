package io.qross.model

import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue}

import io.qross.core.DataTable
import io.qross.ext.Output
import io.qross.jdbc.DataSource
import io.qross.time.DateTime

object NoteRecorder {

    val LOGS = new ConcurrentLinkedQueue[NoteLog]()
    private var stamp = System.currentTimeMillis()

    def SPAN: Long = System.currentTimeMillis() - stamp

    def of(noteId: Long, procId: Long): NoteRecorder = {
        new NoteRecorder(noteId, procId)
    }

    def save(): Unit = synchronized {
        if (NoteRecorder.LOGS.size() > 0) {
            var i = 0
            val ds = DataSource.QROSS
            ds.setBatchCommand(s"INSERT INTO qross_notes_logs (note_id, process_id, log_type, log_text, create_time) VALUES (?, ?, ?, ?, ?)")
            while(NoteRecorder.LOGS.size() > 0 && i < 10000) {
                val log = NoteRecorder.LOGS.poll()
                ds.addBatch(log.noteId, log.procId, log.logType, log.logText, log.logTime)
                i += 1
            }
            ds.executeBatchUpdate()
            ds.close()

            stamp = System.currentTimeMillis()
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

class NoteRecorder(val noteId: Long, val procId: Long) {

    //记录被调度任务运行过程中的输出流
    def out(info: String): NoteRecorder = {
        record("INFO", info)
    }

    //记录被调度任务运行过程中的错误流
    def err(error: String): NoteRecorder = {
        record("ERROR", error)
    }

    //记录被调度任务在执行前后的信息
    def log(info: String): NoteRecorder = {
        Output.writeMessage(info)
        record("LOG", info)
    }

    //记录被调度任务在执行前后的警告信息，会被记录成系统日志
    def warn(warning: String): NoteRecorder = {
        Output.writeWarning(warning)
        record("WARN", warning)
    }

    //记录被调度任务在执行前后的重要信息，会被记录成系统日志
    def debug(message: String): NoteRecorder = {
        Output.writeDebugging(message)
        record("DEBUG", message)
    }

    private def record(seal: String, text: String): NoteRecorder = {
        NoteRecorder.LOGS.add(new NoteLog(noteId, procId, seal, text))
        if (NoteRecorder.LOGS.size() >= 1000 || NoteRecorder.SPAN >= 2000) {
            NoteRecorder.save()
        }
        this
    }

    def dispose(): Unit = {
        NoteRecorder.save()
    }
}

class NoteLog(var noteId: Long, var procId: Long, var logType: String = "INFO", var logText: String = "", var logTime: String = DateTime.now.getString("yyyy-MM-dd HH:mm:ss")) {
    
    private val limit = 65535
    
    if (logType != "INFO" && logType != "ERROR") {
        logText = s"$logTime [$logType] $logText"
    }
    if (logText.length > limit) logText = logText.take(limit)

    override def toString: String = {
        s"Note: $noteId, Process: $procId - $logTime [$logType] ${logText.replace("\r", "\\r")}"
    }
}