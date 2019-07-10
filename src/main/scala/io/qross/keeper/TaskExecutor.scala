package io.qross.keeper

import io.qross.core.DataRow
import io.qross.model.{QrossTask, WorkActor}

class TaskExecutor extends WorkActor {

    override def run(command: DataRow): Unit = {
        val task = QrossTask.executeTaskCommand(command)
        if (task.id > 0) {
            sender ! task
        }
    }
}