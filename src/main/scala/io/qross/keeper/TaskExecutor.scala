package io.qross.keeper

import io.qross.core.DataRow
import io.qross.model.{QrossJob, QrossTask, WorkActor}

class TaskExecutor extends WorkActor {

    override def run(command: DataRow): Unit = {
        val task = QrossTask.executeTaskCommand(command)
        if (task.id > 0) {
            sender ! task
        }
        else if (task.id < 0) {
            QrossJob.queueEndlessJob(task.jobId)
        }
    }
}