package io.qross.keeper

import io.qross.model.{QrossTask, Task, TaskStatus, WorkActor}

class TaskChecker extends WorkActor {
    
    private val starter = context.actorSelection("akka://keeper/user/starter")
    
    override def execute(task: Task): Unit = {
        if (task.status == TaskStatus.INITIALIZED) {
            //if ready
            if (QrossTask.checkTaskDependencies(task)) {
                starter ! task.READY
            }
        }
    }
}