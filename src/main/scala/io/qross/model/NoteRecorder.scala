package io.qross.model

import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue}

import io.qross.core.DataTable
import io.qross.ext.Output
import io.qross.fs.FileWriter
import io.qross.jdbc.DataSource
import io.qross.setting.Global
import io.qross.time.DateTime

import scala.collection.mutable

object NoteRecorder {

    val LOGS = new ConcurrentLinkedQueue[NoteLog]()

    private var stamp = System.currentTimeMillis()

    def SPAN: Long = System.currentTimeMillis() - stamp

    def of(noteId: Long, userId: Int): NoteRecorder = {
        new NoteRecorder(noteId, userId)
    }

    def save(): Unit = synchronized {
        if (NoteRecorder.LOGS.size() > 0) {

            val writers = new mutable.HashMap[String, FileWriter]()

            var i = 0
            while(NoteRecorder.LOGS.size() > 0 && i < 10000) {
                val log = NoteRecorder.LOGS.poll()
                if (!writers.contains(log.path)) {
                    writers += log.path -> new FileWriter(Global.QROSS_HOME + s"notes/${log.path}.log", deleteIfExists = false)
                }
                println(log.line.logText)
                writers(log.path).writeObjectLine(log.line)

                i += 1
            }

            writers.values.foreach(_.close())
        }

        stamp = System.currentTimeMillis()
    }
}

class NoteRecorder(val noteId: Long, val userId: Int) {

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
        NoteRecorder.LOGS.add(new NoteLog(noteId, userId, new NoteLogLine(seal, text)))
        if (NoteRecorder.LOGS.size() >= 1000 || NoteRecorder.SPAN >= 2000) {
            NoteRecorder.save()
        }
        this
    }

    def dispose(): Unit = {
        NoteRecorder.save()
    }
}

class NoteLog(val noteId: Long, val userId: Int, val line: NoteLogLine) {
    val path: String = s"$userId/$noteId"
}

class NoteLogLine(val logType: String = "INFO", var logText: String = "", val logTime: String = DateTime.now.getString("yyyy-MM-dd HH:mm:ss")) {
    if (logType != "INFO" && logType != "ERROR") {
        logText = s"$logTime [$logType] $logText"
    }

//    override def toString: String = {
//        s"""{"logType":"$logType","logText":"${logText.replace("\"", """\"""")}","logTime":"$logTime"}"""
//    }
}