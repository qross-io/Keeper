package io.qross.model

import io.qross.util._

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
            FileWriter(Global.QROSS_KEEPER_HOME + "logs/" + DateTime.now.getString("yyyyMMdd/HH") + ".log", deleteFileIfExists = false)
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
            val error = new KeeperException()
            val ds = new DataSource()
            ds.setBatchCommand(s"INSERT INTO qross_keeper_exceptions (exception, create_date) VALUES (?, ?)")
            exceptions.foreach(line => {
                if (!error.merge(line)) {
                    ds.addBatch(error.exception, error.createDate)
                    
                    error.exception = line.exception
                    error.createDate = line.createDate
                }
            })
            if (error.nonEmpty) {
                ds.addBatch(error.exception, error.createDate)
            }
            ds.executeBatchUpdate()
    
            //email
            val map = ds.executeHashMap("SELECT conf_key, conf_value FROM qross_conf WHERE conf_key IN ('EMAIL_NOTIFICATION', 'COMPANY_NAME', 'EMAIL_EXCEPTIONS_TO_DEVELOPER')")
            if (map("EMAIL_NOTIFICATION") == "yes") {
                OpenResourceFile("/templates/exception.html")
                    .replace("${companyName}", map("COMPANY_NAME"))
                    .replace("${exceptions}", KeeperException.toHTML(exceptions))
                    .writeEmail(s"KEEPER EXCEPTION: ${map("COMPANY_NAME")} ${DateTime.now.toString}")
                    .to(User.getUsers("master"))
                    .cc(if (map("EMAIL_EXCEPTIONS_TO_DEVELOPER") == "yes") "Developer<garfi-wu@outlook.com>" else "")
                    .send()
            }
            map.clear()
            
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
    
    def nonEmpty: Boolean = exception != ""
    
    def merge(keeperException: KeeperException): Boolean = {
        var merged = false
        if (this.exception == "") {
            this.exception = keeperException.exception
            this.createDate = keeperException.createDate
        }
        else if (this.createDate == keeperException.createDate) {
            val text = this.exception + "\r" + keeperException.exception
            if (text.length < 65535) {
                this.exception = text
                merged = true
            }
        }
        merged
    }
}