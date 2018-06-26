package io.qross.model

import io.qross.util.{DataSource, DataTable, DateTime}

object TaskLogger {
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

case class TaskLogger(jobId: Int, taskId: Long, commandId: Int, actionId: Long) {
    
    val logs = DataTable()
    
    var tick: String = DateTime.now.getString("yyyy-MM-dd HH:mm:ss")
    
    def log(line: String): Unit = {
        this.add("INFO", line)
    }
    
    def err(line: String): Unit = {
        this.add("ERROR", line)
    }
    
    private def add(logType: String, logText: String): Unit = {
        
        val now: String = DateTime.now.getString("yyyy-MM-dd HH:mm:ss")
        if (now != tick) {
            save()
            tick = now
        }
    
        logs.insertRow(
            "log_type" -> logType,
            "log_text" -> (if (logText.length < 65535) logText else logText.take(65535)))
    }
    
    private def save(): Unit = {
        val ds = new DataSource()
        if (logs.nonEmpty) {
            var TYPE = "INFO"
            var LINE = ""
            
            ds.setBatchCommand(s"INSERT INTO qross_tasks_logs (job_id, task_id, command_id, action_id, log_type, log_text, create_time) VALUES ($jobId, $taskId, $commandId, $actionId, ?, ?, '$tick')")
            logs.foreach(row => {
                val logType = row.getString("log_type")
                val logText = row.getString("log_text")
                
                if (TYPE != logType) {
                    if (LINE != "") {
                        ds.addBatch(TYPE, LINE)
                        LINE = ""
                    }
                    TYPE = logType
                }
                    
                if (LINE == "") {
                    LINE = logText
                }
                else if (LINE.length + logText.length > 65535) {
                    ds.addBatch(TYPE, LINE)
                    LINE = logText
                }
                else {
                    LINE += "\r" + logText
                }
            })
            logs.clear()
            
            
            if (LINE != "") {
                ds.addBatch(TYPE, LINE)
            }
            ds.executeBatchUpdate()
        }
        ds.close()
    }
    
    def close(): Unit = {
        save()
    }
}