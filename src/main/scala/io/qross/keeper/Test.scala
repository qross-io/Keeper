package io.qross.keeper

import io.qross.model.QrossTask
import io.qross.util.{DateTime, HDFS, HDFSReader, Properties}

object Test {
    def main(args: Array[String]): Unit = {
    
        //Properties.loadAll()
    
        println(Properties.get("hello1"))
        println(Properties.get("hello"))
        println(Properties.get("world"))
        println(Properties.get("mysql.qross"))
        println(Properties.get("testKey"))
        println(Properties.get("mysql.oa"))
        
        
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
