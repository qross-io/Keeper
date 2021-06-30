package io.qross.keeper
import akka.actor.Props
import akka.routing.BalancingPool
import io.qross.ext.Output
import io.qross.model._
import io.qross.net.Http
import io.qross.ext.TypeExt._

class TaskStarter extends WorkActor {
    
    private val executor = context.actorOf(Props[TaskExecutor].withRouter(new BalancingPool(Workshop.MAX)), "executor")
    private val producer = context.actorSelection("akka://keeper/user/producer")
    
    override def beat(tick: String): Unit = {

        executor ! Tick(tick)

        Workshop.delay()

        val address = Keeper.NODE_ADDRESS

        QrossTask.checkTasksStatus(tick)
            .foreach(task => {
                task.status match {
                    case TaskStatus.INITIALIZED => producer ! task
                    case TaskStatus.READY =>
                        if (address == task.address) {
                            runTask(task)
                        }
                        else {
                            try {
                                Http.PUT(s"""http://${task.address}/task/start/${task.id}?jobId=${task.jobId}&taskTime=${task.taskTime}&recordTime=${task.recordTime}&terminus=yes""").request()
                            }
                            catch {
                                case e: java.net.ConnectException =>
                                    //disconnect the unreachable node
                                    Qross.disconnect(task.address, e)
                                    //还在当前机器运行
                                    runTask(task)
                                case e: Exception => e.printReferMessage()
                            }
                        }
                    case _ =>
                }
            })
    }

    //接收从 Producer 和 Checker 发来的消息
    override def execute(task: Task): Unit = {
        runTask(task)
    }

    private def runTask(task: Task): Unit = {
        QrossTask.getTaskCommandsToExecute(task)
            .foreach(row => executor ! TaskCommand(row))
                .clear()
    }
}