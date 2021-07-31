package io.qross.model

import io.qross.time.DateTime

//jobId = downstream job id
class JobDependency(val jobId: Int, val taskTimeFormat: String, val status: String = "success") {
    private var taskTime: String = ""
    def setUpstreamTaskTime(taskTime: String): JobDependency = {
        this.taskTime = taskTime
        this
    }

    def getTaskTime: String = new DateTime(taskTime).getString(taskTimeFormat)
}