package io.qross.keeper

import io.qross.model.{QrossTask, Task, WorkActor}
import io.qross.util.DataRow

class TaskExecutor extends WorkActor {

    override def run(command: DataRow): Unit = {
        val task = QrossTask.executeTaskCommand(command)
        if (task.id > 0) {
            sender ! task
        }
    }
}