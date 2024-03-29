package io.qross.keeper

import akka.actor.{ActorRef, ActorSelection, Props}
import akka.routing.BalancingPool
import io.qross.ext.TypeExt._
import io.qross.keeper.TaskProducer._
import io.qross.model._
import io.qross.net.Http
import io.qross.setting.{Environment, Global}
import io.qross.model.Qross.HashMap$Nodes

import scala.collection.mutable
import scala.util.control.Breaks._

class TaskProducer extends WorkActor {
    
    private val checker = context.actorOf(Props[TaskChecker].withRouter(new BalancingPool(Environment.cpuThreads)), "checker")
    private val starter = context.actorSelection("akka://keeper/user/starter")
        
    override def setup(): Unit = {

        Workshop.delay()

        QrossTask.complementTasks().distributeTo(checker, starter)
    }
    
    override def beat(tick: String): Unit = {
        //acknowledge
        Keeper.TO_BE_ACK -= tick

        checker ! Tick(tick)

        //延时一点时间，按节点的忙碌程度确定到底延时多长，越闲得延时越短，反之越长，最长 1 秒钟
        Workshop.delay()

        QrossTask.createAndInitializeTasks(tick).distributeTo(checker, starter)
    }

    //接收或 message 或 router 发来的消息
    override def execute(task: Task): Unit = {
        task.status match {
            case TaskStatus.INITIALIZED => checker ! task
            case TaskStatus.READY => starter ! task
            case _ =>
        }
    }
}

object TaskProducer {
    implicit class ArrayBuffer$Tasks(tasks: mutable.ArrayBuffer[Task]) {
        def distributeTo(checker: ActorRef, starter: ActorSelection): Unit = {
            if (tasks.nonEmpty) {

                val nodes = Qross.availableNodes
                val address = Keeper.NODE_ADDRESS

                if (nodes.size == 1) {
                    tasks.foreach(task => {
                        task.status match {
                            case TaskStatus.INITIALIZED => checker ! task
                            case TaskStatus.READY => starter ! task
                            case _ =>
                        }
                    })
                }
                else {

                    tasks.foreach(task => {

                        var node = nodes.freest()

                        if (node == address) {
                            task.status match {
                                case TaskStatus.INITIALIZED => checker ! task
                                case TaskStatus.READY => starter ! task
                                case _ =>
                            }
                        }
                        else {
                            val action = task.status match {
                                case TaskStatus.INITIALIZED => "check"
                                case TaskStatus.READY => "start"
                                case _ => "check"
                            }

                            breakable {
                                while (node != address) {
                                    try {
                                        val result = Http.PUT(s"""http://$node/task/$action/${task.id}?token=${Global.KEEPER_HTTP_TOKEN}&jobId=${task.jobId}&taskTime=${task.taskTime.encodeURL()}&recordTime=${task.recordTime.encodeURL()}""").request()
                                        if (result == "accept") {
                                            break
                                        }
                                        else {
                                            node = nodes.freest()
                                        }
                                    }
                                    catch {
                                        case e: java.net.ConnectException =>
                                            Qross.disconnect(node, e)
                                            nodes -= node //remove disconnected node
                                            node = nodes.freest()
                                        case e: Exception => e.printStackTrace()//e.printReferMessage()
                                    }
                                }
                            }

                            if (node == address) {
                                task.status match {
                                    case TaskStatus.INITIALIZED => checker ! task
                                    case TaskStatus.READY => starter ! task
                                    case _ =>
                                }
                            }
                        }
                    })
                }
            }
        }
    }
}