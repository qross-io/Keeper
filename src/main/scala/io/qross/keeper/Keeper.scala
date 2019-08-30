package io.qross.keeper

import akka.actor.{ActorSystem, PoisonPill, Props}
import io.qross.ext.Output
import io.qross.model.{Qross, Tick}
import io.qross.setting.Global
import io.qross.setting.Properties
import io.qross.time.{DateTime, Timer}
import io.qross.fs.FilePath._
import scala.collection.immutable.List
import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits._

object Keeper {
    
    //to be acknowledge for TaskProducer
    val TO_BE_ACK = new mutable.HashSet[String]()
    
    def main(args: Array[String]): Unit = {
        
        //check and load properties
        for (arg <- args) {
            Properties.loadLocalFile(arg.locate())
        }
        
        Qross.start()
        
        val system = ActorSystem("keeper")
        val actors = List(
            system.actorOf(Props[Messager], "messager"),
            system.actorOf(Props[TaskProducer], "producer"),
            system.actorOf(Props[TaskStarter], "starter"),
            system.actorOf(Props[NoteProcessor], "processor"),
            system.actorOf(Props[TaskLogger], "logger"),
            system.actorOf(Props[Monitor], "monitor")
        )
        
        while (!Global.QUIT_ON_NEXT_BEAT) {
            // mm:00
            Timer.sleep()
            Qross.beat("Keeper")
    
            //retransmission if producer hasn't received tick
            TO_BE_ACK.foreach(minute => {
                actors(1) ! Tick(minute) //actors(1) = TaskProducer
            })
        
            val minute = DateTime.now.getString("yyyyMMddHHmm00")
            //to be ack
            TO_BE_ACK += minute
            //send tick
            actors.foreach(actor => {
                actor ! Tick(minute)
            })
        }
        
        for(actor <- actors) {
            actor ! PoisonPill
        }

        Qross.quit("Keeper")

        Output.writeDebugging("System will wait for all actors to finish their work, or you can kill Keeper manually if it stuck.")

        Qross.waitAndStop()

        system.terminate().onComplete(_ => Output.writeDebugging("Qross Keeper shut down!"))
    }
}