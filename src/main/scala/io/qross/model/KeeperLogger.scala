package io.qross.model

import io.qross.ext.Output
import io.qross.fs.{FileWriter, ResourceFile}
import io.qross.jdbc.DataSource
import io.qross.setting.Global
import io.qross.time.DateTime

import scala.collection.mutable

class KeeperLogger {
    
    private val logs = new mutable.ArrayBuffer[String]()
    private val exceptions = new mutable.ArrayBuffer[KeeperException]()
    private var tick = System.currentTimeMillis()
    private var timer = 0L
    
    def debug(info: String): Unit = {
        logs += info
        if (System.currentTimeMillis() - tick > 5000 || logs.size >= 100) {
            save()
        }
    }
    
    def err(exception: String): Unit = {
        debug(exception)
        
        exceptions += new KeeperException(exception)
        timer = System.currentTimeMillis()
    }

    def overtime: Boolean = {
        timer > 0 && System.currentTimeMillis() - timer > 5000
    }
    
    private def save(): Unit = {
        if (logs.nonEmpty) {
            new FileWriter(Global.QROSS_HOME + "logs/" + DateTime.now.getString("yyyyMMdd/HH") + ".log", deleteIfExists = false)
                .writeLines(logs)
                .close()
    
            Output.writeMessage(s"Record ${logs.size} lines into log file.")
            logs.clear()
        }
    
        tick = System.currentTimeMillis()
    }
    
    def store(): Unit = {
        if (exceptions.nonEmpty) {
            //save to database
            val ds = DataSource.QROSS
            ds.setBatchCommand(s"INSERT INTO qross_keeper_exceptions (exception, create_date) VALUES (?, ?)")
            exceptions.foreach(line => {
                ds.addBatch(line.exception, line.createDate)
            })
            ds.executeBatchUpdate()
    
            //email
            val info = ds.executeDataRow("SELECT conf_key, conf_value FROM qross_conf WHERE conf_key IN ('EMAIL_NOTIFICATION', 'COMPANY_NAME', 'EMAIL_EXCEPTIONS_TO_DEVELOPER')")
            if (info.getBoolean("EMAIL_NOTIFICATION")) {
                ResourceFile.open("/templates/exception.html")
                    .replace("#{companyName}", info.getString("COMPANY_NAME"))
                    .replace("#{exceptions}", KeeperException.toHTML(exceptions))
                    .writeEmail(s"KEEPER EXCEPTION: ${info.getString("COMPANY_NAME")} ${DateTime.now.toString}")
                    .to(QrossUser.getUsers("master"))
                    .cc(if (info.getBoolean("EMAIL_EXCEPTIONS_TO_DEVELOPER")) "Developer<garfi-wu@outlook.com>" else "")
                    .send()
            }

            ds.close()
            
            timer = 0L
            exceptions.clear()
        }
    }
    
    def close(): Unit = {
        save()
        store()
    }
}

object KeeperException {
    def toHTML(exceptions: mutable.ArrayBuffer[KeeperException]): String = {
        val sb = new StringBuilder()
        exceptions.foreach(line => {
            sb.append("<div class='ERROR'>")
            sb.append(line.exception.replace("\r", "<br/>").replace("\t", "&nbsp;&nbsp;&nbsp;&nbsp;"))
            sb.append("</div>")
        })
        sb.toString()
    }
}

class KeeperException(var exception: String = "") {
    if (exception.length > 65535) exception = exception.take(65535)
    var createDate: String = DateTime.now.getString("yyyy-MM-dd")
}