package io.qross.model

import io.qross.ext.Output.{writeDebugging, writeLineWithSeal}
import io.qross.jdbc.DataSource
import io.qross.time.{CronExp, DateTime}
import io.qross.ext.TypeExt._

import scala.collection.mutable
import scala.util.Try
import scala.util.control.Breaks._

object QrossJob {

    //任务总表，每分钟更新，新增的添加，移除的删除，修改的更新
    //每秒钟更新，value减 1，当tick等于0时，触发任务
    //当任务执行完成时, 重置tick时间
    val ENDLESS_JOBS: mutable.HashMap[Int, EndlessJob] = new mutable.HashMap[Int, EndlessJob]()

    def refreshEndlessJobs(): Unit = {

        val ds = DataSource.QROSS

        val jobs = ds.queryDataMap[Int, String](s"SELECT id AS job_id, cron_exp FROM qross_jobs WHERE job_type='${JobType.ENDLESS}' AND enabled='yes'")

        //remove
        ENDLESS_JOBS
                .keys
                .foreach(jobId => {
                    if (!jobs.contains(jobId)) {
                        ENDLESS_JOBS -= jobId
                        writeDebugging(s"Endless job $jobId has removed from list!")
                    }
                })

        //update & renew
        jobs.foreach(job => {
            if (ENDLESS_JOBS.contains(job._1)) {
                if (ENDLESS_JOBS(job._1).update(job._2)) {
                    writeDebugging(s"Endless job ${job._1} has updated!")
                }
            }
            else {
                ENDLESS_JOBS += job._1 -> new EndlessJob(job._2)
                writeDebugging(s"Endless job ${job._1} has added to list!")
            }
        })

        //check stuck job
        ENDLESS_JOBS
            .foreach(job => {
                if (job._2.executing) {
                    if (!ds.executeExists(s"SELECT id FROM qross_tasks WHERE job_id=${job._1} AND status IN ('${TaskStatus.NEW}', '${TaskStatus.INITIALIZED}', '${TaskStatus.READY}', '${TaskStatus.EXECUTING}')")) {
                        job._2.renew()
                        writeDebugging(s"Endless job ${job._1} has restored from stuck!")
                    }
                }
                else if (job._2.suspending) {
                    //检查挂起的调度是否满足运行条件
                    job._2.renew()
                }
            })

        //wait a while

        ds.executeNonQuery("UPDATE qross_keeper_beats SET last_beat_time=NOW() WHERE actor_name='Repeater'")
        ds.close()
        writeLineWithSeal("SYSTEM", "Repeater beat!")
    }

    def tickEndlessJobs(): List[Int] = {
        ENDLESS_JOBS
                .filter(_._2.waiting)
                .map(job => {
                    if (job._2.tick()) {
                        job._2.execute()
                        job._1
                    }
                    else {
                        0
                    }
                })
                .filter(_ > 0)
                .toList
    }

    def queueEndlessJob(jobId: Int): Unit = synchronized {
        if (ENDLESS_JOBS.contains(jobId)) {
            ENDLESS_JOBS(jobId).renew()
            writeDebugging(s"Endless job $jobId has renewed!")
        }
    }
}

//interCronExp 格式为  interval-seconds@cron_exp; interval-seconds@cron_exp; ...
//默认间隔为5秒
class EndlessJob(private val interCronExp: String) {

    private val intervals = new mutable.HashMap[String, Int]()
    private var cronExp = ""

    //默认状态是 suspending
    private var apart: Int = -1
    private var status: String = "waiting"

    update(interCronExp)
    renew()

    def update(newInterCronExp: String): Boolean = {
        if (cronExp != newInterCronExp) {
            cronExp = newInterCronExp
            intervals.clear()
            newInterCronExp.split(";")
                .foreach(interCron => {
                    if (interCron.contains("@")) {
                        intervals += interCron.takeAfter("@").trim() -> Try(interCron.takeBefore("@").trim().toInt).getOrElse(5)
                    }
                    else {
                        intervals += "DEFAULT" -> Try(interCron.trim().toInt).getOrElse(5)
                    }
                })

            true
        }
        else {
            false
        }
    }

    //每次执行完成之后更新时间间隔
    def renew(): Unit = {
        var matched = false
        breakable {
            intervals.foreach(interCron => {
                if (interCron._1 != "DEFAULT") {
                    if (CronExp(interCron._1).matches(DateTime.now.setSecond(0))) {
                        this.apart = interCron._2
                        matched = true
                        break
                    }
                }
            })
        }

        if (!matched && intervals.contains("DEFAULT")) {
            this.apart = intervals("DEFAULT")
        }

        this.status = "waiting"
    }

    def tick(): Boolean = {
        if (apart > 0) {
            apart -= 1
        }
        ready
    }

    def execute(): Unit = {
        apart = -1
        status = "executing"
    }

    def executing: Boolean = status == "executing"
    def waiting: Boolean = apart > 0 && status == "waiting"
    def ready: Boolean = apart == 0 && status == "waiting"
    def suspending: Boolean = apart < 0 && status == "waiting"
}