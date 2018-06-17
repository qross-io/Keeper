package io.qross.keeper

import io.qross.model.{Global, KeeperLogger}
import io.qross.util.DateTime

import scala.sys.process._

object Protector {
    def main(args: Array[String]): Unit = {
    
        val command = s"hadoop jar ${Global.QROSS_HOME}qross-keeper-${Global.QROSS_VERSION}.jar io.qross.keeper.Keeper /data/config/qinling/databases.properties"
        
        val logger = new KeeperLogger()
        logger.debug(s"${DateTime.now.getString("yyyy-MM-dd HH:mm:ss")} [INFO] Qross Keeper starting.")
        val exitValue = command.!(ProcessLogger(out => {
                if (out.contains("[DEBUG]")) {
                    logger.debug(out)
                }
            }, err => {
                logger.debug(err)
            }))
        logger.debug(s"${DateTime.now.getString("yyyy-MM-dd HH:mm:ss")} [INFO] Qross Keeper quit with exitValue $exitValue")
        logger.close()
    }
}
