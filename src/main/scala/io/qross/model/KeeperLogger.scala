package io.qross.model

import io.qross.util.{DateTime, FileWriter}

import scala.collection.mutable

class KeeperLogger {
    
    private val box = new mutable.ArrayBuffer[String]()
    private var count = 0
    
    def debug(info: String): Unit = {
        box += info
        count += 1
        
        if (count >= 100) {
            save()
        }
    }
    
    private def save(): Unit = {
        FileWriter(Global.QROSS_KEEPER_HOME + "logs/" + DateTime.now.getString("yyyyMMddHH") + ".log", deleteFileIfExists = false)
            .writeLines(box)
                .close()
        box.clear()
        count = 0
    }
    
    def close(): Unit = {
        if (count > 0) {
            save()
        }
    }
}
