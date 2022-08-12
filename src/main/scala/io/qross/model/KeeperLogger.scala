package io.qross.model

import io.qross.ext.Output
import io.qross.ext.TypeExt._
import io.qross.fs.{FileWriter, ResourceFile}
import io.qross.jdbc.DataSource
import io.qross.keeper.{Keeper, Setting}
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
        """2\d{3}[/-]\d{2}[/-]\d{2} \d{2}:\d{2}:\d{2}""".r.findFirstIn(exception) match {
            case Some(_) => debug(exception)
            case None => debug(DateTime.now + " [ERROR] " + exception)
        }
        
        exceptions += new KeeperException(exception)
        timer = System.currentTimeMillis()
    }

    def overtime: Boolean = {
        timer > 0 && System.currentTimeMillis() - timer > 5000
    }
    
    private def save(): Unit = {
        if (logs.nonEmpty) {
            new FileWriter(Global.QROSS_HOME + "keeper/logs/" + DateTime.now.getString("yyyyMMdd/HH") + ".log", deleteIfExists = false)
                .writeLines(logs)
                .close()
    
            Output.writeMessage(s"${DateTime.now} [INFO] Record ${logs.size} line(s) into log file.")
            logs.clear()
        }
    
        tick = System.currentTimeMillis()
    }
    
    def store(): Unit = {
        if (exceptions.nonEmpty) {
            //save to database
            val ds = DataSource.QROSS
            ds.setBatchCommand(s"INSERT INTO qross_keeper_exceptions (node_address, exception, create_date) VALUES ('${Keeper.NODE_ADDRESS}', ?, ?)")
            exceptions.foreach(line => {
                ds.addBatch(line.text, line.createDate)
            })
            ds.executeBatchUpdate()

            ds.executeDataTable(s"SELECT event_function, event_value, event_option, '${Keeper.NODE_ADDRESS}' AS node_address FROM qross_keeper_events WHERE event_name='onNodeErrorOccur' AND enabled='yes' AND event_value IS NOT NULL AND event_value<>''")
                .foreach(row => {
                    val recipients = {
                        val to = row.getString("event_value")
                        if (to.contains("_KEEPER") && to.contains("_MASTER")) {
                            "(role='master' OR role='keeper')"
                        }
                        else if (to.contains("_KEEPER")) {
                            "role='keeper'"
                        }
                        else {
                            "role='master'"
                        }
                    }
                    row.getString("event_function") match {
                        case "SEND_MAIL" =>
                            if (Global.EMAIL_NOTIFICATION) {
                                ResourceFile.open("/templates/exception.html")
                                    .replace("#{companyName}", Setting.COMPANY_NAME)
                                    .replace("#{exceptions}", KeeperException.toHTML(exceptions))
                                    .writeEmail(s"KEEPER EXCEPTION: ${Setting.COMPANY_NAME} ${DateTime.now.toString}")
                                    .to(ds.executeSingleValue(s"SELECT GROUP_CONCAT(CONCAT(fullname, '<', email, '>')) AS recipients FROM qross_users WHERE $recipients AND enabled='yes'").asText(""))
                                    .send()
                            }
                        case "REQUEST_API" =>
                            row.set("exceptions", exceptions.map(_.text).mkString("\r").encodeURL())
                            Qross.requestApi(row)
                        case "EXECUTE_SCRIPT" =>
                            row.set("exceptions", exceptions.map(_.text).mkString("\r"))
                            Qross.executeScript(row)
                        case _ =>
                    }
                })

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
            sb.append(line.text.replace("\r", "<br/>").replace("\t", "&nbsp;&nbsp;&nbsp;&nbsp;"))
            sb.append("</div>")
        })
        sb.toString()
    }
}

class KeeperException(exception: String = "") {
    val text = if (exception.length > 65535) exception.take(65535) else exception
    val createDate: String = DateTime.now.getString("yyyy-MM-dd")
}