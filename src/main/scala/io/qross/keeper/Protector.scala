package io.qross.keeper

import io.qross.model.{Global, KeeperLogger}
import io.qross.util.{DateTime, Output}

import scala.sys.process._

object Protector {
    def main(args: Array[String]): Unit = {
    
        var debug = false
        var bash = s"hadoop jar"
        var properties = ""
        
        for (i <- args.indices) {
            args(i).toLowerCase() match {
                case "--debug" => debug = true
                case "--client" => bash = s"${Global.JAVA_BIN_HOME}java -cp"
                case "--properties" if i + 1 < args.length => properties = args(i+1)
                case _ =>
            }
        }
        
        val command = s"$bash ${Global.QROSS_HOME}qross-keeper-${Global.QROSS_VERSION}.jar io.qross.keeper.Keeper $properties"
        Output.writeMessage("Run: " + command)
        
        val logger = new KeeperLogger()
        logger.debug(s"${DateTime.now.getString("yyyy-MM-dd HH:mm:ss")} [DEBUG] Qross Keeper starting.")
        val exitValue = command.!(ProcessLogger(out => {
                if (debug && out.contains("[DEBUG]")) {
                    logger.debug(out)
                }
                else {
                    println(out)
                }
            }, err => {
                logger.debug(err)
            }))
        logger.debug(s"${DateTime.now.getString("yyyy-MM-dd HH:mm:ss")} [DEBUG] Qross Keeper quit with exitValue $exitValue")
        Output.writeMessage("Exit: " + command)
        
        logger.close()
    }
}
