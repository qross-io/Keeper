package io.qross.model

import io.qross.core.DataHub
import io.qross.ext.Output.{writeDebugging, writeLineWithSeal}
import io.qross.keeper.Keeper
import io.qross.ext.TypeExt._
import io.qross.jdbc.DataSource
import io.qross.time.{CronExp, DateTime}

import scala.collection.mutable
import scala.util.Try
import scala.util.control.Breaks.{break, breakable}

object EndlessJob {

    def renewEndlessJob(cronExp: String): Int = {
        val intervals = new mutable.HashMap[String, Int]()
        cronExp.split(";")
            .foreach(interCron => {
                if (interCron.contains("@")) {
                    intervals += interCron.takeAfter("@").trim() -> Try(interCron.takeBefore("@").trim().toInt).getOrElse(5)
                }
                else {
                    intervals += "DEFAULT" -> Try(interCron.trim().toInt).getOrElse(5)
                }
            })

        var matched = false
        var apart = -1

        breakable {
            intervals.foreach(interCron => {
                if (interCron._1 != "DEFAULT") {
                    if (CronExp(interCron._1).matches(DateTime.now.setSecond(0))) {
                        apart = interCron._2
                        matched = true
                        break
                    }
                }
            })
        }

        if (!matched && intervals.contains("DEFAULT")) {
            apart = intervals("DEFAULT")
        }

        apart
    }

    def refreshEndlessJobs(tick: String): Boolean = {

        val dh = DataHub.QROSS
        val address = Keeper.NODE_ADDRESS

        val locked = dh.executeNonQuery(s"UPDATE qross_keeper_locks SET node_address='$address', tick='$tick', lock_time=NOW() WHERE lock_name='REFRESH-ENDLESS-JOBS' AND tick<>'$tick'") == 1
        if (locked) {
            dh.get(s"SELECT id AS job_id, cron_exp FROM qross_jobs WHERE job_type='${JobType.ENDLESS}' AND enabled='yes'")
                .put(s"INSERT INTO qross_endless_jobs_queue (job_id, cron_exp, refresh_time) VALUES (#job_id, '#cron_exp', '$tick') ON DUPLICATE KEY UPDATE cron_exp='#cron_exp', refresh_time='$tick' ")
                .set(s"DELETE FROM qross_endless_jobs_queue WHERE refresh_time<>'$tick'")

            //stuck
            dh.get(s"SELECT A.job_id, A.cron_exp, A.apart, A.status FROM qross_endless_jobs_queue A INNER JOIN qross_tasks B ON A.job_id=B.job_id AND B.status NOT IN ('${TaskStatus.NEW}', '${TaskStatus.INITIALIZED}', '${TaskStatus.READY}', '${TaskStatus.EXECUTING}') WHERE A.status='executing'")
                .foreach(row => {
                    val apart = renewEndlessJob(row.getString("cron_exp"))
                    row.set("apart", apart)
                    row.set("status", if (apart > 0) "waiting" else "suspending")
                }).put("UPDATE qross_endless_jobs_queue SET status='#status', apart=#aprt WHERE job_id=#job_id")

            //suspending
            dh.get("SELECT job_id, cron_exp, status, apart FROM qross_endless_jobs_queue WHERE status='suspending'")
                .foreach(row => {
                    val apart = renewEndlessJob(row.getString("cron_exp"))
                    row.set("apart", apart)
                    row.set("status", if (apart > 0) "waiting" else "suspending")
                }).put("UPDATE qross_endless_jobs_queue SET status='#status', apart=#aprt WHERE job_id=#job_id")
        }

        writeLineWithSeal("SYSTEM", s"<$address> Repeater beat!")
        dh.executeNonQuery(s"UPDATE qross_keeper_beats SET last_beat_time=NOW() WHERE node_address='$address' AND actor_name='Repeater'")

        dh.close()

        locked
    }

    def tickEndlessJobs(): List[Int] = {
        DataHub.QROSS
            .set("UPDATE qross_endless_jobs_queue SET apart=apart-1 WHERE status='waiting' AND apart>0")
            .get("SELECT id, job_id FROM qross_endless_jobs_queue WHERE status='waiting' AND apart=0")
                .put("UPDATE qross_endless_jobs_queue SET status='executing' WHERE id=#id")
                .takeOut(true)
                .toList[Int]
    }

    def queueEndlessJob(jobId: Int): Unit = synchronized {
        val ds = DataSource.QROSS
        val cron = ds.executeSingleValue(s"SELECT cron_exp FROM qross_endless_jobs_queue WHERE job_id=$jobId").asText("")
        val apart = if (cron.nonEmpty) renewEndlessJob(cron) else -1
        ds.executeNonQuery(s"UPDATE qross_endless_jobs_queue SET status='${ if (apart > -1) "waiting" else "suspending"}', apart=$apart  WHERE job_id=$jobId")
        ds.close()
        writeDebugging(s"Endless job $jobId has renewed!")
    }
}
