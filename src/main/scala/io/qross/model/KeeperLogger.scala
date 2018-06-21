package io.qross.model

import io.qross.util.{DateTime, FileWriter, Output}

import scala.collection.mutable

class KeeperLogger {
    
    private val logs = new mutable.ArrayBuffer[String]()
    private var tick = System.currentTimeMillis()
    
    def debug(info: String): Unit = {
        logs += info
        println(info)
        
        if (System.currentTimeMillis() - tick > 2000) {
            save()
        }
    }
    
    private def save(): Unit = {
        if (logs.nonEmpty) {
            FileWriter(Global.QROSS_KEEPER_HOME + "logs/" + DateTime.now.getString("yyyyMMdd/HH") + ".log", deleteFileIfExists = false)
                .writeLines(logs)
                    .close()
            
            Output.writeMessage(s"Record ${logs.size} lines info log file.")
            logs.clear()
        }
        
        tick = System.currentTimeMillis()
    }
    
    def close(): Unit = {
        save()
    }
}
