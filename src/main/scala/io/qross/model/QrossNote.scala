package io.qross.model

import io.qross.core.DataHub
import io.qross.jdbc.DataSource
import io.qross.setting.Global
import io.qross.time.Timer

import scala.collection.mutable
import scala.sys.process._

object QrossNote {

    val QUERYING: mutable.HashSet[Long] = new mutable.HashSet[Long]()
    val TO_BE_KILLED: mutable.HashSet[Long] = new mutable.HashSet[Long]()

    //run note
    def query(noteId: Long, userId: Int): Unit = {

        val commandText = Global.JAVA_BIN_HOME + s"java -jar ${Global.QROSS_HOME}qross-worker-${Global.QROSS_VERSION}.jar --note ${noteId}"

        val logger = NoteRecorder.of(noteId, userId: Int)
        val stamp = System.currentTimeMillis()

        QUERYING += noteId

        logger.debug(s"Note $noteId is running.")

        var exitValue = 1

        try {
            val process = commandText.run(ProcessLogger(out => {
                logger.out(out)
            }, err => {
                logger.err(err)
            }))

            while (process.isAlive()) {
                //if killed
                if (TO_BE_KILLED.contains(noteId)) {
                    TO_BE_KILLED -= noteId
                    process.destroy() //kill it
                    exitValue = -2

                    logger.warn(s"Note $noteId has been KILLED.")
                }

                Timer.sleep(1000)
            }

            exitValue = process.exitValue()
        }
        catch {
            case e: Exception =>
                e.printStackTrace()

                val buf = new java.io.ByteArrayOutputStream()
                e.printStackTrace(new java.io.PrintWriter(buf, true))
                logger.err(buf.toString())
                buf.close()

                logger.err(s"Note $noteId is exceptional: ${e.getMessage}")

                exitValue = 2
        }

        val status = exitValue match {
            case 0 => "SUCCESS"
            case -2 => "KILLED"
            case _ => "FAILED"
        }

        QUERYING -= noteId

        logger.debug(s"Note $noteId has finished with status $status, elapsed ${ (System.currentTimeMillis() - stamp) / 1000 }s.").dispose()
    }
}
