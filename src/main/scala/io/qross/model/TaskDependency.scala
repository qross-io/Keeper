package io.qross.model

import java.io.File
import java.util.regex.Pattern

import io.qross.util._

import scala.collection.mutable
import scala.util.{Success, Try}

object TaskDependency {
    
    def main(args: Array[String]): Unit = {
    
        var s = "{\"dataSource\": \"mysql.bi_platform\","
            s += "selectSQL\": \"${jobId}\","
            s += "updateSQL\": \"UPDATE BD2800_executions SET status=(SELECT status FROM qross_tasks WHERE id=${taskId}) WHERE task_id=${taskId}\"}"
        
        parseDependencyValue("1", "2345", s, "20180504101000").foreach(println)
    }
    
    def parseDependencyValue(jobId: String, taskId: String, dependencyValue: String, taskTime: String): List[String] = {
    
        var content = dependencyValue
        content = content.replace("${jobId}", jobId)
        content = content.replace("${taskId}", taskId)
        
        if (content.contains("${") && content.contains("}")) {
            val values = new mutable.TreeSet[String]()
            val semi = new java.util.LinkedList[String]()
            semi.offer(content)
            
            while (!semi.isEmpty) {
                val value = semi.poll()
                val ahead = value.substring(0, value.indexOf("${"))
                var exp = value.substring(value.indexOf("${") + 2)
                val latter = exp.substring(exp.indexOf("}") + 1)
                exp = exp.substring(0, exp.indexOf("}"))
                
                DateTime(taskTime).sharp(exp).foreach(value => {
                        val replacement = ahead + value + latter
                        if (latter.contains("${") && latter.contains("}")) {
                            semi.offer(replacement)
                        }
                        else {
                            values += replacement
                        }
                    })
            }
            
            values.toList
        }
        else {
            List[String](content)
        }
    }

    def check(dependencyType: String, dependencyValue: String): (String, String) =  {
        
        var ready = "no"
        val conf = Json(dependencyValue).findDataRow("/")
        
        dependencyType.toUpperCase() match {
            
            /*
            {
                "dataSource": "source name"
                "selectSQL": "select SQL"
                "updateSQL": "update SQL, support $ and # place holder"
            }
             */
            case "SQL" =>
                    val dh = new DataHub(conf.getString("dataSource"))
                    val table = dh.executeDataTable(conf.getString("selectSQL"))
                    if (table.nonEmpty) {
                        ready = "yes"
                        if (conf.contains("updateSQL") && conf.getString("updateSQL") != "") {
                            dh.buffer(table).put(conf.getString("updateSQL"))
                        }
                        conf.set("SELECT", table.count())
                    }
                    else {
                        conf.set("SELECT", "EMPTY")
                    }
                    table.clear()
                    dh.close()
            /*
            {
                "path": ""
                "minAmount": 1
                "minLength": "10M"
                "lastModifiedTimeSpan": "5m"
            }
             */
            case "HDFS" =>
                val files = HDFS.list(conf.getString("path"))
                conf.set("amount", files.size)
                if (files.size >= conf.getLongOption("minAmount").getOrElse(1L)) {
                    var minSize = 0L
                    var lastModified = 0L
                    for (file <- files) {
                        if (minSize == 0 || minSize < file.size) {
                            minSize = file.size
                        }
                        if (lastModified == 0 || lastModified > file.lastModificationTime) {
                            lastModified = file.lastModificationTime
                        }
                    }
                    conf.set("size", FileLength.toHumanizedString(minSize))
                    conf.set("lastModified", lastModified / 1000)
                
                    ready = "yes"
                    
                    if (conf.contains("minLength")) {
                        if (minSize < FileLength.toByteLength(conf.getString("minLength"))) {
                            ready = "no"
                        }
                    }
                    
                    if (conf.contains("lastModifiedTimeSpan")) {
                        if (System.currentTimeMillis() - lastModified < conf.getLongOption("lastModifiedTimeSpan").getOrElse(60L) * 1000L) {
                            ready = "no"
                        }
                    }
                }
        
            /*
            {
                "url": "",
                "post": "",
                "path": "",
                "operator": "",
                "value": ""
            }
            */
            case "API" =>
                val json = Json().readURL(conf.getString("url"), conf.getString("post", ""))
                
                val currentValue = json.findValue(conf.getString("path")).toString
                val compareValue = conf.getString("value")
                if (conf.getString("operator").trim match {
                    case "=" | "==" => currentValue == compareValue
                    case "!=" => currentValue != compareValue
                    case "^=" => currentValue.startsWith(compareValue)
                    case "$=" => currentValue.endsWith(compareValue)
                    case "#=" => currentValue.contains(compareValue)
                    case "*=" => Pattern.matches(compareValue, currentValue) //regex match
                    //case "?=" => Pattern.matches(compareValue, currentValue) //regex match
                    //case "*=" => Pattern.compile(compareValue, Pattern.CASE_INSENSITIVE).matcher(currentValue).matches() //regex math
                    case operator =>
                        (Try(currentValue.toDouble), Try(compareValue.toDouble)) match {
                            case (Success(v1), Success(v2)) =>
                                operator match {
                                    case ">" => v1 > v2
                                    case ">=" => v1 >= v2
                                    case "<" => v1 < v2
                                    case "<=" => v1 < v2
                                    case _ => false
                                }
                            case _ => false
                        }
                }) {
                    ready = "yes"
                }
        
            /*
            {
                "jobId": 123,
                "taskTime": "datetime sharp expression"
            }
            */
            case "TASK" =>
                if (DataSource.queryExists("SELECT id FROM qross_tasks WHERE job_id=? AND task_time=? AND status='finished'",
                        conf.getIntOption("jobId").getOrElse(0),
                        conf.getString("taskTime"))) {
                    ready = "yes"
                }
        
            /*
            {
                "path": "",
                "minLength": "123K"
            }
            */
            case "FILE" =>
                val file = new File(conf.getString("path"))
                if (file.exists()) {
                    conf.set("exists", "yes")
                    conf.set("length", FileLength.toHumanizedString(file.length()))
                    if (file.length() >= FileLength.toByteLength(conf.getString("minLength"))) {
                        ready = "yes"
                    }
                }
                else {
                    conf.set("exists", "no")
                }
                
            case _ =>
        }
    
        (ready, conf.toString)
    }
}