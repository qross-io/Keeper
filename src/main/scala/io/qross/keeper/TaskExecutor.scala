package io.qross.keeper

import io.qross.model.{QrossAction, QrossTask, Task, WorkActor}
import io.qross.util.DataRow

class TaskExecutor extends WorkActor {
    
    
    
    override def run(command: DataRow): Unit = {
        val taskId = QrossAction.executeTaskCommand(command)
        if (taskId > 0) {
            sender ! Task(taskId).EXECUTING
        }
    }
}