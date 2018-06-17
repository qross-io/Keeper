package io.qross.keeper

import io.qross.model.QrossTask
import io.qross.util.{DateTime, Properties}

object Test {
    def main(args: Array[String]): Unit = {
    
        println(DateTime.of(2018, 3, 1).toEpochSecond)
        println(DateTime.of(2018, 5, 1).toEpochSecond)
        println(DateTime.of(2018, 5, 2).toEpochSecond)
        //Properties.loadAll(args: _*)
        //QrossTask.checkTaskDependencies(542973L)
        //println(DateTime.of(2018, 3, 1).toEpochSecond)
    }
}
