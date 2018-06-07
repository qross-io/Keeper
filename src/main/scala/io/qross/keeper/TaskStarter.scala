package io.qross.keeper
import akka.actor.{ActorRef, Props}
import akka.routing.BalancingPool
import io.qross.model.{Global, QrossTask, TaskCommand, WorkActor}
import io.qross.util.DateTime

class TaskStarter extends WorkActor {
    
    private val executor = context.actorOf(Props[TaskExecutor].withRouter(new BalancingPool(Global.CORES * 2)), "executor")
    
    override def beat(tick: String): Unit = {
        QrossTask.checkOvertimeOfActions(tick)
    }
    
    override def execute(taskId: Long, taskStatus: String): Unit = {
        QrossTask.getTaskCommandsToExecute(taskId, taskStatus)
            .foreach(row => executor ! TaskCommand(row))
                .clear()
    }
}
