package io.qross.trash


import io.qross.core.DataHub
import io.qross.model._
import io.qross.time.{ChronExp, DateTime}
import io.qross.ext.TypeExt._
import io.qross.fs.FileWriter
import io.qross.jdbc.DataSource
import io.qross.net.Json
import io.qross.pql.PQL

import scala.collection.mutable
import io.qross.pql.Solver._
import io.qross.setting.Properties
import io.qross.pql.PQL._


object Test {

    def main(args: Array[String]): Unit = {

        PQL.runFile("/pql/metadata_quality.sql")
        //ChronExp("WEEKLY 7 10-23:0/2").getNextTick(DateTime.now).print

        //QrossTask.getTaskCommandsToExecute(Task(12L, TaskStatus.READY).of(544).at("20190927101800", "2019-09-27 10:18:00.018"))

        //TaskStorage.store(652, 95501, "2019-10-18 14:51:09", DataHub.QROSS)
        //Qross.checkBeatsAndRecords()

//        TaskStorage.store(1, 2038, "2019-10-15 10:09:15",
//            DataSource.QROSS.queryDataTable(s"SELECT * FROM qross_tasks_logs WHERE task_id=2038")
//        )


//        implicit val system: ActorSystem = ActorSystem("api-server")
//        implicit val materializer: ActorMaterializer = ActorMaterializer()
//        implicit val executionContext: ExecutionContextExecutor = system.dispatcher
//
//
//        val bindingFuture = Http().bindAndHandle(Router.rests(system),"0.0.0.0",7700)
//        StdIn.readLine()
//        bindingFuture.flatMap(_.unbind()).onComplete(_=>system.terminate())

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
