package io.qross.keeper

import akka.actor.{ActorSystem, PoisonPill, Props}
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import io.qross.ext.Output
import io.qross.fs.Path._
import io.qross.model.{Qross, Tick}
import io.qross.setting.{Environment, Global, Properties}
import io.qross.time.{DateTime, Timer}

import scala.collection.immutable.List
import scala.collection.mutable
import scala.concurrent.ExecutionContextExecutor
import scala.util.Try

object Keeper {
    
    //to be acknowledge for TaskProducer
    val TO_BE_ACK = new mutable.HashSet[String]()
    val HOST_ADDRESS: String = Environment.localHostAddress  //ip
    var HOST_PORT: Int = Global.KEEPER_HTTP_PORT  //port
    def NODE_ADDRESS: String = HOST_ADDRESS + ":" + HOST_PORT

    def main(args: Array[String]): Unit = {

        //set a different port
        if (args.nonEmpty) {
            HOST_PORT = Try(args(0).toInt).getOrElse(Global.KEEPER_HTTP_PORT)
        }

        Qross.start()

        implicit val system: ActorSystem = ActorSystem("keeper")
        val actors = List(
            system.actorOf(Props[TaskProducer], "producer"),
            system.actorOf(Props[TaskStarter], "starter"),
            system.actorOf(Props[NoteProcessor], "processor"),
            system.actorOf(Props[TaskLogger], "logger"),
            system.actorOf(Props[Repeater], "repeater"),
            system.actorOf(Props[Inspector], "inspector")
        )

        //for akka http
        implicit val materializer: ActorMaterializer = ActorMaterializer()
        implicit val executionContext: ExecutionContextExecutor = system.dispatcher
        //akka http
        val bindingFuture = Http().bindAndHandle(Router.rests(system),"0.0.0.0", HOST_PORT)

        while (!Setting.QUIT_ON_NEXT_BEAT) {
            // mm:00
            Timer.sleepToNextMinute()

            Qross.beat("Keeper")

            //retransmission if producer hasn't received tick
            TO_BE_ACK.foreach(minute => {
                actors.head ! Tick(minute) //actors.head = TaskProducer
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

        Qross.shutting()

        Output.writeLineWithSeal("SYSTEM", "System will wait for all actors to finish their work, or you can kill Keeper manually if it stuck.")

        Qross.waitAndStop()

        Qross.shutdown()

        bindingFuture.flatMap(_.unbind()).onComplete(_ => {
            system.terminate().onComplete(_ => Output.writeDebugging("Qross Keeper shut down!"))
        })
    }
}