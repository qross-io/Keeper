package io.qross.trash


import io.qross.model.{QrossTask, TaskOverall}
import io.qross.util.Json.ListExt
import io.qross.util.{CronExp, DateTime, OpenResourceFile, Output}

import scala.collection.mutable.ArrayBuffer

case class Test(var name: String = "", list: ArrayBuffer[Int] = new ArrayBuffer[Int]()) {

    name = "tom"
    list += 1
    list += 2
    list += 3
}

object Test {
    def main(args: Array[String]): Unit = {

        println("12.345999999199999999991999999999991".toDouble)

        /*QrossTask.createInstantTask("1234567",
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

        //val dh = new DataHub()
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
