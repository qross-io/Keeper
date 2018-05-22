package io.qross.keeper

import io.qross.model.{QrossTask, Task, TaskStatus, WorkActor}

class TaskChecker extends WorkActor {
    
    private val starter = context.actorSelection("akka://keeper/user/starter")
    
    override def execute(taskId: Long, taskStatus: String): Unit = {
        if (taskStatus == TaskStatus.INITIALIZED) {
            //if ready
            if (QrossTask.checkTaskDependencies(taskId)) {
                starter ! Task(taskId).READY
            }
        }
    }
}