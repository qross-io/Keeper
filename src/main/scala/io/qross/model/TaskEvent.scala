package io.qross.model

import io.qross.util.{DataRow, DataTable, Json, OpenResourceFile}

object TaskEvent {

    def execute(content: DataRow): Unit = {

    }

    def sendMail(status: String, row: DataRow, logs: DataTable): Unit = {
        if (Global.EMAIL_NOTIFICATION) {
            val receivers = row.getString("receivers")
            val upperStatus = status.toUpperCase()
            if (receivers != "") {
                OpenResourceFile(s"/templates/$status.html")
                    .replace("${status}", upperStatus)
                    .replaceWith(row)
                    .replace(s"${logs}", TaskRecord.toHTML(logs))
                    .writeEmail(s"$upperStatus: ${row.getString("title")} ${row.getString("task_time")} - JobID: ${row.getString("job_id")} - TaskID: ${row.getString("task_id")}")
                    .to(if (receivers.contains("(OWNER)")) row.getString("owner") else "")
                    .to(if (receivers.contains("(MASTER)")) Global.MASTER_USER_GROUP else "")
                    .to(if (receivers.contains("(KEEPER)")) Global.KEEPER_USER_GROUP else "")
                    .send()
            }

            TaskRecord.of(row.getInt("job_id"), row.getLong("task_id"))
                    .log(s"Task ${row.getLong("task_id")} of job ${row.getInt("job_id")} send mail on $status")
        }
    }

    def requestApi(status: String, row: DataRow): Unit = {
        var api = status match {
            case TaskStatus.CHECKING_LIMIT => Global.API_ON_TASK_CHECKING_LIMIT
            case TaskStatus.FAILED => Global.API_ON_TASK_FAILED
            case TaskStatus.TIMEOUT => Global.API_ON_TASK_TIMEOUT
            case TaskStatus.INCORRECT => Global.API_ON_TASK_INCORRECT
            case TaskStatus.FINISHED => Global.API_ON_TASK_FINISHED
            case _ => ""
        }

        if (api != "") {
            var method = "GET"
            var path = "/"

            if (api.contains("@")) {
                method = api.substring(0, api.indexOf("@")).trim.toUpperCase()
                api = api.substring(api.indexOf("@") + 1).trim
            }
            if (api.contains("->")) {
                path = api.substring(api.indexOf("->") + 2).trim
                api = api.substring(0, api.indexOf("->")).trim
            }

            api = api.replace("${jobId}", row.getString("job_id"))
                        .replace("${taskId}", row.getString("task_id"))
                        .replace("${title}", row.getString("title"))
                        .replace("${retryTimes}", row.getString("retry_times"))
                        .replace("${retryLimit}", row.getString("retry_limit"))
                        .replace("${owner}", row.getString("owner"))
                        .replace("${taskTime}", row.getString("taskTime"))
                        .replace("${status}", status)

            val result = try {
                Json.fromURL(api, method).findValue(path)
            }
            catch {
                case e: Exception => e.getMessage
                case _: Throwable => ""
            }

            TaskRecord.of(row.getInt("job_id"), row.getLong("task_id"))
                    .log(s"Task ${row.getLong("task_id")} of job ${row.getInt("job_id")} request api on $status")
        }
    }
}
