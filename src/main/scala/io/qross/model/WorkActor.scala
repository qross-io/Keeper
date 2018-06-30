package io.qross.model

import akka.actor.Actor
import io.qross.util.{DataRow, DateTime}
import io.qross.util.Output._

class WorkActor extends Actor {
    
    val actorName: String = this.getClass.getSimpleName
    var tick: DateTime = _
    
    def setup(): Unit = {}
    def beat(tick: String): Unit = Qross.beat(actorName)
    def execute(taskId: Long, taskStatus: String): Unit = {}
    def run(taskCommand: DataRow): Unit = {}
    def cleanup(): Unit = {}
    
    override def preStart(): Unit = {
        Qross.run(actorName, self.path.toString)
        setup()
    }
    
    override def postStop(): Unit = {
        Qross.quit(actorName)
        cleanup()
    }
    
    override def receive: Receive = {
        case Tick(minute) => beat(minute)
        case Task(taskId, status) =>
                writeDebugging(s"$actorName receive ${status.toUpperCase()} TASK $taskId")
                execute(taskId, status)
        case TaskCommand(row) =>
            writeDebugging(s" $actorName receive runnable COMMAND $row")
                run(row)
        case _ => writeMessage(s"$actorName receive INVALID MESSAGE")
    }
}
