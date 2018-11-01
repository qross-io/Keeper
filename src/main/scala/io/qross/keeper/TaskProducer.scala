package io.qross.keeper

import akka.actor.Props
import akka.routing.BalancingPool
import io.qross.model._
import io.qross.util.DateTime

class TaskProducer extends WorkActor {
    
    private val checker = context.actorOf(Props[TaskChecker].withRouter(new BalancingPool(Global.CORES)), "checker")
    private val starter = context.actorSelection("akka://keeper/user/starter")
        
    override def setup(): Unit = QrossTask.complementTasks()
    
    override def beat(tick: String): Unit = {
        //acknowledge
        Keeper.TO_BE_ACK -= tick
        
        QrossTask.createAndInitializeTasks(tick).foreach(row => {
            val task = Task(row.getLong("task_id"), row.getString("status")).of(row.getInt("job_id")).at(row.getString("task_time"), row.getString("record_time"))
            task.status match {
                case TaskStatus.INITIALIZED => checker ! task
                case TaskStatus.READY => starter ! task
                case _ =>
            }
        }).clear()
        
        checker ! Tick(tick)
    }
    
    override def execute(task: Task): Unit = {
        task.status match {
            case TaskStatus.INITIALIZED => checker ! task
            case TaskStatus.READY => starter ! task
            case _ =>
        }
    }
}