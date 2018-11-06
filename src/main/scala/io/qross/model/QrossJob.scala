package io.qross.model

import io.qross.util.{CronExp, DataSource, DateTime}
import io.qross.util.Json._

object QrossJob {
    
    //get complement tasks for master when enable a job
    def tickTasks(jobId: Int, queryId: String): Unit = {
        val ds = new DataSource()
        val job = ds.executeDataRow(s"SELECT cron_exp, CAST(switch_time AS CHAR) AS switch_time FROM qross_jobs WHERE id=$jobId")
        val json = CronExp.getTicks(
                job.getString("cron_exp"),
                job.getString("switch_time"),
                DateTime.now.getString("yyyy-MM-dd HH:mm:ss")).toJson.replace("'", "''")
        ds.executeNonQuery(s"INSERT INTO qross_query_result (query_id, result) VALUES ('$queryId', '$json')")
        ds.close()
    }

    //手工创建时返回某一个区间段的可执行任务, 考虑移到master js
    def manualTickTasks(messageText: String, queryId: String): Unit = {
        val jobId = messageText.substring(0, messageText.indexOf(":"))
        val beginTime = messageText.substring(messageText.indexOf(":") + 1, messageText.indexOf("#"))
        val endTime = messageText.substring(messageText.indexOf("#"))

        val ds = new DataSource()
        val cronExp = ds.executeSingleValue(s"SELECT cron_exp FROM qross_jobs WHERE id=$jobId").getOrElse("")
        val json = CronExp.getTicks(cronExp, beginTime, endTime).toJson.replace("'", "''")
        ds.executeNonQuery(s"INSERT INTO qross_query_result (query_id, result) VALUES ('$queryId', '$json')")
        ds.close()
    }
}
