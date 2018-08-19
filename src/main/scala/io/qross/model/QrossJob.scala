package io.qross.model

import io.qross.util.{CronExp, DataSource, DateTime}
import io.qross.util.Json._

object QrossJob {
    
    //get complement tasks for master when enable a job
    def tickTasks(jobId: Int, queryId: String): Unit = {
        
        val ds = new DataSource()
        val job = ds.executeDataRow(s"SELECT cron_exp, switch_time FROM qross_jobs WHERE id=$jobId")
        val json = CronExp.getTicks(
                job.getString("cron_exp"),
                job.getString("switch_time"),
                DateTime.now.getString("yyyy-MM-dd HH:mm:ss")).toJson.replace("'", "''")
        ds.executeNonQuery(s"INSERT INTO qross_query_result (query_id, result) VALUES ('$queryId', '$json')")
        ds.close()
        //CronExp.getTicks()
        //lastTaskTime
        //CronExp.getTicks()
    }
}
