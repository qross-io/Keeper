package io.qross.keeper

import akka.actor.{ActorSystem, PoisonPill, Props}
import io.qross.model.{Beats, Global, TaskRecord, Tick}
import io.qross.util.{DateTime, Output, Properties, Timer}

import scala.collection.immutable.List
import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits._

object Keeper {
    
    //to be acknowledge for TaskProducer
    val TO_BE_ACK = new mutable.HashSet[String]()
    
    def main(args: Array[String]): Unit = {
        
        //check properties
        Properties.loadAll(args: _*)
        //Global.recordStart()
        
        val actorName: String = "Keeper"
        Beats.start(actorName)
        
        val system = ActorSystem(actorName.toLowerCase())
        val actors = List(
            system.actorOf(Props[Messager], "messager"),
            system.actorOf(Props[TaskProducer], "producer"),
            system.actorOf(Props[TaskStarter], "starter"),
            system.actorOf(Props[TaskLogger], "logger")
        )
        
        while (!Global.QUIT_ON_NEXT_BEAT) {
            // mm:00
            Timer.sleep()
            Beats.beat(actorName)
    
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
        
        system.terminate().onComplete(_ => {
            //save left logs
            Beats.quit(actorName)
        })
    }
}