package io.qross.keeper

import akka.actor.{ActorSystem, PoisonPill, Props}
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import io.qross.ext.Output
import io.qross.fs.Path._
import io.qross.model.{Qross, Tick}
import io.qross.setting.{Global, Properties}
import io.qross.time.{DateTime, Timer}

import scala.collection.immutable.List
import scala.collection.mutable
import scala.concurrent.ExecutionContextExecutor

object Keeper {
    
    //to be acknowledge for TaskProducer
    val TO_BE_ACK = new mutable.HashSet[String]()
    
    def main(args: Array[String]): Unit = {

        //check and load properties
        for (arg <- args) {
            Properties.loadLocalFile(arg.locate())
        }

        Qross.start()

        implicit val system: ActorSystem = ActorSystem("keeper")
        val actors = List(
            //system.actorOf(Props[Messenger], "messenger"),
            system.actorOf(Props[TaskProducer], "producer"),
            system.actorOf(Props[TaskStarter], "starter"),
            system.actorOf(Props[NoteProcessor], "processor"),
            system.actorOf(Props[TaskLogger], "logger"),
            system.actorOf(Props[Repeater], "repeater")
        )

        //for akka http
        implicit val materializer: ActorMaterializer = ActorMaterializer()
        implicit val executionContext: ExecutionContextExecutor = system.dispatcher
        //akka http
        val bindingFuture = Http().bindAndHandle(Router.rests(system),"0.0.0.0", Global.KEEPER_HTTP_PORT)

        while (!Setting.QUIT_ON_NEXT_BEAT) {
            // mm:00
            Timer.sleepToNextMinute()
            Qross.beat("Keeper")
    
            //retransmission if producer hasn't received tick
            TO_BE_ACK.foreach(minute => {
                actors(1) ! Tick(minute) //actors(1) = TaskProducer
            })
        
            val minute = DateTime.now.getTickValue
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

        Output.writeLineWithSeal("SYSTEM", "System will wait for all actors to finish their work, or you can kill Keeper manually if it stuck.")

        Qross.waitAndStop()

        bindingFuture.flatMap(_.unbind()).onComplete(_ => {
            system.terminate().onComplete(_ => Output.writeDebugging("Qross Keeper shut down!"))
        })
    }
}