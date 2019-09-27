package io.qross.keeper
import akka.actor.Props
import akka.routing.BalancingPool
import io.qross.model._
import io.qross.setting.{Environment, Global}

class TaskStarter extends WorkActor {
    
    private val executor = context.actorOf(Props[TaskExecutor].withRouter(new BalancingPool(Environment.cpuThreads * Global.CONCURRENT_BY_CPU_CORES)), "executor")
    
    override def beat(tick: String): Unit = {
        executor ! Tick(tick)
        QrossTask.checkTasksStatus(tick)
    }
    
    override def execute(task: Task): Unit = {
        QrossTask.getTaskCommandsToExecute(task)
            .foreach(row => executor ! TaskCommand(row))
                .clear()
    }
}