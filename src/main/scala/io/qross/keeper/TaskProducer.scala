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
        
        QrossTask.createAndInitializeTasks(tick).foreach(row =>
            row.getString("status") match {
                case TaskStatus.INITIALIZED => checker ! Task(row.getLong("task_id")).INITIALIZED
                case TaskStatus.READY => starter ! Task(row.getLong("task_id")).READY
                case _ =>
            }).clear()
    }
    
    override def execute(taskId: Long, taskStatus: String): Unit = {
        QrossTask.restartTask(taskId, taskStatus) match {
            case TaskStatus.INITIALIZED => checker ! Task(taskId).INITIALIZED
            case TaskStatus.READY => starter ! Task(taskId).READY
            case _ =>
        }
    }
}