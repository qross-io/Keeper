package io.qross.keeper

import io.qross.model.{Global, KeeperLogger}
import io.qross.util.{DateTime, Output, Properties}

import scala.sys.process._

object Protector {
    def main(args: Array[String]): Unit = {
        
        Properties.loadAll()
        
        val bash = if (Global.HADOOP_AND_HIVE_ENABLED) "hadoop jar" else s"${Global.JAVA_BIN_HOME}java -cp"
        val command = s"$bash ${Global.QROSS_HOME}qross-keeper-${Global.QROSS_VERSION}.jar io.qross.keeper.Keeper ${Global.QROSS_HOME}qross.properties"
        Output.writeMessage("Run: " + command)
        
        val logger = new KeeperLogger()
        logger.debug(s"${DateTime.now.getString("yyyy-MM-dd HH:mm:ss")} [DEBUG] Qross Keeper starting.")
        val exitValue = command.!(ProcessLogger(
            out => {
                println(out)
                if (Global.LOGS_LEVEL == "DEBUG") {
                    logger.debug(out)
                }
    
                if (logger.overtime) logger.store()
            },
            err => {
                System.err.println(err)
                logger.err(err)
            }))
        logger.debug(s"${DateTime.now.getString("yyyy-MM-dd HH:mm:ss")} [DEBUG] Qross Keeper quit with exitValue $exitValue")
        logger.close()
    
        Output.writeMessage("Exit: " + command)
    }
}
