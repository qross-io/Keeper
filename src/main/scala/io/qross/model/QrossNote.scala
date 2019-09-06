package io.qross.model

import io.qross.core.DataHub
import io.qross.jdbc.DataSource
import io.qross.setting.Global
import io.qross.time.{DateTime, Timer}

import scala.collection.mutable
import scala.sys.process._

object QrossNote {

    val TO_BE_KILLED: mutable.HashSet[Long] = new mutable.HashSet[Long]()

    //create a instance of Note
    def process(noteId: Long, executor: Int = 0): Process = {
        val ds = DataSource.QROSS
        ds.executeNonQuery(s"INSERT INTO qross_processes (note_id, executor) VALUES ($noteId, $executor)")
        val procId = ds.executeSingleValue(s"SELECT id FROM qross_processes WHERE note_id=$noteId ORDER BY id DESC LIMIT 1").asInteger
        ds.close()

        io.qross.model.Process(noteId, procId)
    }

    //run process
    def perform(proc: Process): Unit = {

        val commandText = Global.JAVA_BIN_HOME + s"java -jar ${Global.QROSS_HOME}qross-worker-${Global.QROSS_VERSION}.jar --note ${proc.noteId}"

        val logger = NoteRecorder.of(proc.noteId, proc.id)
        val stamp = System.currentTimeMillis()

        val dh = DataHub.QROSS

        //delete redundant logs
        dh.get(s"SELECT id FROM qross_processes WHERE note_id=${proc.noteId}") //ORDER BY id DESC LIMIT 3,1
            .put(s"DELETE FROM qross_notes_logs WHERE process_id=#id")

        dh.set(s"UPDATE qross_processes SET start_time=NOW(), status='${ProcessStatus.EXECUTING}' WHERE id=${proc.id}")

        logger.debug(s"Note ${proc.noteId} is running. Process ID is ${proc.id}")

        var exitValue = 1

        try {
            val process = commandText.run(ProcessLogger(out => {
                logger.out(out)
            }, err => {
                logger.err(err)
            }))

            while (process.isAlive()) {
                //if killed
                if (TO_BE_KILLED.contains(proc.id)) {
                    TO_BE_KILLED -= proc.id
                    process.destroy() //kill it
                    exitValue = -2

                    logger.warn(s"Note ${proc.noteId} (Process ${proc.id}) has been KILLED.")
                }

                Timer.sleep(1)
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

                logger.err(s"Note ${proc.noteId} (Process ${proc.id}) is exceptional: ${e.getMessage}")

                exitValue = 2
        }

        val status = exitValue match {
            case 0 => ProcessStatus.SUCCESS
            case -2 => ProcessStatus.KILLED
            case _ => ProcessStatus.FAILED
        }

        dh.set(s"UPDATE qross_processes SET status='$status', finish_time=NOW(), spent=TIMESTAMPDIFF(SECOND, start_time, NOW()) WHERE id=${proc.id}")

        dh.close()

        logger.debug(s"Note ${proc.noteId} has finished with status ${status.toUpperCase}, elapsed ${ (System.currentTimeMillis() - stamp) / 1000 }s.").dispose()
    }
}
