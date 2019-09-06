package io.qross.trash


import java.net.InetAddress

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import io.qross.time._
import akka.http.scaladsl.{Http, server}
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import io.qross.keeper.Router
import io.qross.net.Json

import scala.concurrent.ExecutionContextExecutor
import scala.io.StdIn

object Test {
    def main(args: Array[String]): Unit = {


        implicit val system: ActorSystem = ActorSystem("api-server")
        implicit val materializer: ActorMaterializer = ActorMaterializer()
        implicit val executionContext: ExecutionContextExecutor = system.dispatcher


        val bindingFuture = Http().bindAndHandle(Router.rests(system),"0.0.0.0",7700)
        StdIn.readLine()
        bindingFuture.flatMap(_.unbind()).onComplete(_=>system.terminate())

        //Server.startServer("localhost",8080,actorSystem)

        //Qross.checkBeatsAndRecords()

        /*

        QrossTask.createInstantTask("1234567",
            """{
              "jobId": 1,
               "dag": "",
               "params": "",
               "commands": "",
               "delay": 5
              }""") */

        //Output.writeLine(OpenResourceFile("/templates/new.html").toString)

        //TaskOverall.of(1700L).store()

        //Output.writeMessage("NEXT TICK: " + CronExp.parse("0 0 1 L * ? *").getNextTick(DateTime.now))

        //writeMessage("NEXT TICK: " + CronExp.parse("0 58 7/2 * * FRI *").getNextTick(dateTime))

        //writeMessage("NEXT TICK: " + CronExp.parse("0 7 8,10 * * ? *").getNextTick(dateTime))

        //val list = List[String]("1", "2", "3")

        //val dh = DataHub.QROSS
        //dh.close()
        /*println(DateTime.now.getString("yyyyMMdd/HH"))

        //QrossTask.checkTaskDependencies(542973L)
        //println(DateTime.of(2018, 3, 1).toEpochSecond)

        HDFS.list(args(1)).foreach(hdfs => {
            val reader = new HDFSReader(hdfs.path)
            var line = ""
            var count = 0
            while(reader.hasNextLine) {
                line = reader.readLine
                count += 1
            }
            reader.close()

            println(hdfs.path + " # " + count)
        })
        */

    }
}
