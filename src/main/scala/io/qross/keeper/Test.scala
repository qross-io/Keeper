package io.qross.keeper

import io.qross.model.QrossTask
import io.qross.util.{DateTime, HDFS, HDFSReader, Properties}

object Test {
    def main(args: Array[String]): Unit = {
    
        /*println(DateTime.now.getString("yyyyMMdd/HH"))
        
        Properties.loadAll(args(0))
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
