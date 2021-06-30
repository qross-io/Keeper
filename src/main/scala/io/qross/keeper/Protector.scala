package io.qross.keeper

import io.qross.ext.Output
import io.qross.model.KeeperLogger
import io.qross.setting.{Environment, Global, Properties}
import io.qross.time.DateTime

import scala.sys.process._
import scala.util.Try

object Protector {

    def main(args: Array[String]): Unit = {

        if (args.nonEmpty) {
            Keeper.HOST_PORT = Try(args(0).toInt).getOrElse(Global.KEEPER_HTTP_PORT)
        }

        //val bash = if (Setting.HADOOP_AND_HIVE_ENABLED) "hadoop jar" else s"${Global.JAVA_BIN_HOME}java -cp"
        val bash = s"${Global.JAVA_BIN_HOME}java -Dfile.coding=${Global.CHARSET} -cp"
        val command = s"$bash ${Global.QROSS_HOME}qross-keeper-${Global.QROSS_VERSION}.jar io.qross.keeper.Keeper" + (if (args.nonEmpty) s" ${Keeper.HOST_PORT}" else "")
        Output.writeMessage("Run: " + command)
        
        val logger = new KeeperLogger()
        logger.debug(s"${DateTime.now.getString("yyyy-MM-dd HH:mm:ss")} [DEBUG] Qross Keeper starting.")
        val exitValue = command.!(ProcessLogger(
            out => {
                println(out)
                logger.debug(out)
                if (logger.overtime) {
                    logger.store()
                }
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
