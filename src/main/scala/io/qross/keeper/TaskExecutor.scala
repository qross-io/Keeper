package io.qross.keeper

import io.qross.model.{QrossTask, Task, WorkActor}
import io.qross.util.DataRow

class TaskExecutor extends WorkActor {

    override def run(command: DataRow): Unit = {
        val taskId = QrossTask.executeTaskCommand(command)
        if (taskId > 0) {
            sender ! Task(taskId).EXECUTING
        }
    }
}