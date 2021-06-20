package io.qross.model

import io.qross.jdbc.DataSource
import io.qross.setting.Global
import io.qross.time.Timer

import scala.collection.mutable
import scala.sys.process._

object QrossNote {

    val QUERYING: mutable.HashSet[Long] = new mutable.HashSet[Long]()
    val TO_BE_STOPPED: mutable.HashSet[Long] = new mutable.HashSet[Long]()

    //run note
    def query(noteId: Long, userId: Int): Unit = {

        val commandText = Global.JAVA_BIN_HOME + s"java -Dfile.encoding=UTF-8 -jar ${Global.QROSS_HOME}qross-worker-${Global.QROSS_VERSION}.jar --note ${noteId}"

        val logger = NoteRecorder.of(noteId, userId: Int)
        val stamp = System.currentTimeMillis()

        QUERYING += noteId

        DataSource.QROSS.queryUpdate(s"UPDATE qross_notes SET status='querying' WHERE id=$noteId")

        logger.debug(s"Note $noteId is running.")

        var exitValue = 1

        try {
            val process = commandText.run(
                ProcessLogger(out => {
                    logger.out(out)
                }, err => {
                    logger.err(err)
                }))

//            val process = Process(commandText).run(
//                new ProcessIO(
//                    in  => in.close(),
//                    out => {
//                        val reader = new BufferedReader(new InputStreamReader(out, Global.CHARSET))
//                        while (alive) {
//                            logger.out(reader.readLine)
//                        }
//                        reader.close()
//                    },
//                    err => {
//                        scala.io.Source.fromInputStream(err).getLines.foreach(line => {
//                            logger.err(line)
//                        })
//                    })
//            )

            while (process.isAlive()) {
                //if killed
                if (TO_BE_STOPPED.contains(noteId)) {
                    TO_BE_STOPPED -= noteId
                    process.destroy() //kill it
                    exitValue = -2

                    logger.warn(s"Note $noteId has been STOPPED.")
                }

                Timer.sleep(1000)
            }

            if (exitValue != -2) {
                exitValue = process.exitValue()
            }
        }
        catch {
            case e: Exception =>

                //e.printStackTrace()
                //将明细输出到流
                //val buf = new java.io.ByteArrayOutputStream()
                //e.printStackTrace(new java.io.PrintWriter(buf, true))
                //logger.err(buf.toString())
                //buf.close()

                logger.err(s"Exception: ${e.getMessage}")

                exitValue = 2
        }

        val status = exitValue match {
            case 0 => "success"
            case -2 => "stopped"
            case _ => "failed"
        }

        QUERYING -= noteId

        logger.debug(s"Note $noteId has finished with status $status, elapsed ${ (System.currentTimeMillis() - stamp) / 1000 }s.")
            .dispose()

        DataSource.QROSS.queryUpdate(s"UPDATE qross_notes SET status='$status' WHERE id=$noteId")
    }
}
