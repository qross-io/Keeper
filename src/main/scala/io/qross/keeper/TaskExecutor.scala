package io.qross.keeper

import io.qross.core.DataRow
import io.qross.model.{EndlessJob, QrossTask, WorkActor, Workshop}

class TaskExecutor extends WorkActor {

    override def run(command: DataRow): Unit = {

        Workshop.work()

        val task = QrossTask.executeTaskCommand(command)
        if (task.id > 0) {
            sender ! task
        }
        else if (task.id < 0) {
            EndlessJob.queueEndlessJob(task.jobId)
        }

        Workshop.complete()
    }
}