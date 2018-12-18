package io.qross.model;

object TaskEvent {

    def sendMail(status: String, row: DataRow, logs: DataTable = DataTable()): Unit = {

        if (Global.EMAIL_NOTIFICATION) {
            val receivers = row.getString("receivers")
            val upperStatus = status.toUpperCase()
            if (receivers != "") {
                OpenResourceFile(s"/templates/$status.html")
                    .replace("${status}", upperStatus)
                    .replaceWith(row)
                    .replace("${logs}", TaskRecorder.toHTML(logs))
                    .writeEmail(s"$upperStatus: ${row.getString("title")} ${row.getString("task_time")} - JobID: ${row.getString("job_id")} - TaskID: ${row.getString("task_id")}")
                    .to(if (receivers.contains("_OWNER")) row.getString("owner") else "")
                    .cc(if (receivers.contains("_MASTER")) Global.MASTER_USER_GROUP else "")
                    .cc(if (receivers.contains("_KEEPER")) Global.KEEPER_USER_GROUP else "")
                    .send()
            }

            TaskRecorder.of(row.getInt("job_id"), row.getLong("task_id"), row.getString("record_time"))
                    .debug(s"Task ${row.getLong("task_id")} of job ${row.getInt("job_id")} at <${row.getString("record_time")}> sent a mail on task $status")
        }
    }

    def requestApi(status: String, row: DataRow): Unit = {

        var api = row.getString("api")

        if (api == "") {
            api = status match {
                case TaskStatus.NEW => Global.API_ON_TASK_NEW
                case TaskStatus.CHECKING_LIMIT => Global.API_ON_TASK_CHECKING_LIMIT
                case TaskStatus.READY => Global.API_ON_TASK_READY
                case TaskStatus.FAILED => Global.API_ON_TASK_FAILED
                case TaskStatus.TIMEOUT => Global.API_ON_TASK_TIMEOUT
                case TaskStatus.INCORRECT => Global.API_ON_TASK_INCORRECT
                case TaskStatus.FINISHED => Global.API_ON_TASK_FINISHED
                case _ => ""
            }
        }

        if (api != "") {
            var method = "GET"
            var path = "/"

            //api格式 method @ api
            if (api.contains("@")) {
                method = api.substring(0, api.indexOf("@")).trim.toUpperCase()
                api = api.substring(api.indexOf("@") + 1).trim
            }
            if (api.contains("->")) {
                path = api.substring(api.indexOf("->") + 2).trim
                api = api.substring(0, api.indexOf("->")).trim
            }

            api = api.replace("${jobId}", row.getString("job_id", "0"))
                        .replace("${taskId}", row.getString("task_id", "0"))
                        .replace("${commandId}", row.getString("command_id", "0"))
                        .replace("${actionId}", row.getString("action_id", "0"))
                        .replace("${status}", status)
                        .replace("${title}", row.getString("title"))
                        .replace("${retryTimes}", row.getString("retry_times", "0"))
                        .replace("${retryLimit}", row.getString("retry_limit", "0"))
                        .replace("${owner}", row.getString("owner"))
                        .replace("${taskTime}", row.getString("task_time"))
                        .replace("${status}", status)

            val result = try {
                Json.fromURL(api, method).findValue(path)
            }
            catch {
                case e: Exception => e.getMessage
                case _: Throwable => ""
            }

            TaskRecorder.of(row.getInt("job_id"), row.getLong("task_id"), row.getString("record_time"))
                .debug(s"Task ${row.getLong("task_id")} of job ${row.getInt("job_id")} at <${row.getString("record_time")}> requested specified api on task $status, result is { $result }")
        }
    }
}
