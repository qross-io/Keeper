package io.qross.model

import io.qross.ext.Output.{writeDebugging, writeMessage}
import io.qross.jdbc.DataSource
import io.qross.time.{ChronExp, DateTime}
import io.qross.ext.TypeExt._

import scala.collection.mutable

object QrossJob {

    //任务总表，每分钟更新，新增的添加，移除的删除，修改的更新
    //每秒钟更新，value减1，当tick等于0时，触发任务
    //当任务执行完成时, 重置tick时间
    val ENDLESS_JOBS: mutable.HashMap[Int, EndlessJob] = new mutable.HashMap[Int, EndlessJob]()
    
    //get complement tasks for master when enable a job, 考虑移到master中
    def tickTasks(jobId: Int, queryId: String): Unit = {
        val ds = new DataSource()
        val job = ds.executeDataRow(s"SELECT cron_exp, CAST(switch_time AS CHAR) AS switch_time FROM qross_jobs WHERE id=$jobId")
        val json = ChronExp.getTicks(
                job.getString("cron_exp"),
                job.getString("switch_time"),
                DateTime.now.getString("yyyy-MM-dd HH:mm:ss")).toJsonString.replace("'", "''")
        ds.executeNonQuery(s"INSERT INTO qross_query_result (query_id, result) VALUES ('$queryId', '$json')")
        ds.close()
    }

    //手工创建时返回某一个区间段的可执行任务, 考虑移到master js
    def manualTickTasks(messageText: String, queryId: String): Unit = {
        val jobId = messageText.substring(0, messageText.indexOf(":"))
        val beginTime = messageText.substring(messageText.indexOf(":") + 1, messageText.indexOf("#"))
        val endTime = messageText.substring(messageText.indexOf("#"))

        val ds = new DataSource()
        val cronExp = ds.executeSingleValue(s"SELECT cron_exp FROM qross_jobs WHERE id=$jobId").asText
        val json = ChronExp.getTicks(cronExp, beginTime, endTime).toJsonString.replace("'", "''")
        ds.executeNonQuery(s"INSERT INTO qross_query_result (query_id, result) VALUES ('$queryId', '$json')")
        ds.close()
    }

    def refreshEndlessJobs(): Unit = {

        val ds = DataSource.QROSS

        val map = ds.queryDataMap[Int, Int](s"SELECT id AS job_id, intervals FROM qross_jobs WHERE job_type='${JobType.ENDLESS}' AND enabled='yes'")
        //remove
        ENDLESS_JOBS
                .keys
                .foreach(jobId => {
                    if (!map.contains(jobId)) {
                        ENDLESS_JOBS -= jobId
                        writeDebugging(s"Endless job $jobId has removed from list!")
                    }
                })
        //update
        map.foreach(job => {
            if (ENDLESS_JOBS.contains(job._1)) {
                ENDLESS_JOBS(job._1).update(job._2)
                writeDebugging(s"Endless job ${job._1} has updated!")
            }
            else {
                ENDLESS_JOBS += job._1 -> EndlessJob(job._2)
                writeDebugging(s"Endless job ${job._1} has added to list!")
            }
        })
        //check stuck job
        ENDLESS_JOBS.filter(_._2.executing)
                    .foreach(job => {
                        if (!ds.executeExists(s"SELECT id FROM qross_tasks WHERE job_id=${job._1} AND status='${TaskStatus.EXECUTING}'")) {
                            job._2.renew()
                            writeDebugging(s"Endless job ${job._1} has restored from stuck!")
                        }
                    })

        ds.executeNonQuery("UPDATE qross_keeper_beats SET last_beat_time=NOW() WHERE actor_name='TaskProducer'")
        ds.close()
        writeMessage("Repeater beat!")
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

case class EndlessJob(var interval: Int) {
    private var apart: Int = interval
    private var status: String = "waiting"

    def update(gap: Int): Unit = {
        if (gap != this.interval) {
            this.interval = gap
        }
    }

    def renew(): Unit = {
        this.apart = interval
        this.status = "waiting"
    }

    def tick(): Boolean = {
        if (apart > 0) {
            apart -= 1
        }
        ready
    }

    def execute(): Unit = {
        status = "executing"
    }

    def executing: Boolean = status == "executing"
    def waiting: Boolean = status == "waiting"
    def ready: Boolean = apart == 0 && status == "waiting"
}