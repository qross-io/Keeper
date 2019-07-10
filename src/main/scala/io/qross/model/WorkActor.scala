package io.qross.model

import akka.actor.Actor
import io.qross.core.DataRow
import io.qross.ext.Output._

class WorkActor extends Actor {
    
    val actorName: String = this.getClass.getSimpleName

    def setup(): Unit = {}
    def beat(tick: String): Unit = Qross.beat(actorName)
    def execute(task: Task): Unit = {}
    def run(taskCommand: DataRow): Unit = {}
    def process(note: Note): Unit = { }
    def cleanup(): Unit = { }
    
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
        case task: Task =>
            writeDebugging(s"$actorName receive ${task.status.toUpperCase()} TASK ${task.id}")
            execute(task)
        case TaskCommand(row) =>
            writeDebugging(s" $actorName receive runnable command $row")
            run(row)
        case note: Note =>
            writeDebugging(s" $actorName receive note")
            process(note)
        case _ => writeMessage(s"$actorName receive INVALID MESSAGE")
    }
}
