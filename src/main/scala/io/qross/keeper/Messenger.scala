package io.qross.keeper

import io.qross.ext.Output
import io.qross.model._
import io.qross.setting.{Configurations, Global, Properties}
import io.qross.time.{DateTime, Timer}
import io.qross.fs.FilePath._
import io.qross.jdbc.JDBC
import io.qross.ext.TypeExt._

import scala.util.{Success, Try}

class Messenger extends WorkActor {

    private val producer = context.actorSelection("akka://keeper/user/producer")
    private val processor = context.actorSelection("akka://keeper/user/processor")
    //private val starter = context.actorSelection("akka://keeper/user/starter")
    
    override def beat(tick: String): Unit = {
        super.beat(tick)
        
        val nextMinute = new DateTime(tick).plusMinutes(1).toEpochSecond
        do {
            MessageBox.check().foreach(row => {
                val queryId = row.getString("query_id")
                val sender = row.getInt("sender")
                val messageType = row.getString("message_type").toUpperCase
                val messageKey = row.getString("message_key").toUpperCase
                val messageText = row.getString("message_text")
                Output.writeDebugging(s"Got A Message # $queryId # $messageType # $messageKey # $messageText")

                messageType match {
                    case "GLOBAL" => Configurations.set(messageKey, messageText)
                    case "JOB" =>
                        //JOB - COMPLEMENT - job_id
                        //JOB - MANUAL - job_id:begin_time#end_time
                        //JOB - CREATE -
                        // {
                        //      "project_id": 0,
                        //      "title": "",
                        //      "cron_exp": "",
                        //      "closing_time": "",
                        //      "command_text": ""
                        // }
                        messageKey match {
                            case "COMPLEMENT" => QrossJob.tickTasks(messageText.toInt, queryId)
                            case "MANUAL" => QrossJob.manualTickTasks(messageText, queryId)
                            case _ =>
                        }
                    case "TASK" =>
                        //TASK - RESTART - WHOLE@TaskID - WHOLE@123
                        //TASK - RESTART - ^CommandIDs@TaskID - ^1,2,3,4,5@123
                        //TASK - RESTART - ^EXCEPTIONAL@TaskID - ^EXCEPTIONAL@123
                        //TASK - RESTART - CommandIDs@TaskID - 1,2,3,4,5@123
                        //TASK - INSTANT -
                            /*
                            {
                                jobId: 123,
                                dag: "1,2,3",
                                params: "name1:value1,name2:value2",
                                commands: "commandId:commandText##$##commandId:commandText",
                                delay: 0,
                                start_time: 'yyyyMMddHHmm00',
                                creator: @username
                            }
                             */
                        //TASK - KILL - actionId
                        messageKey match {
                            case "RESTART" => producer ! QrossTask.restartTask(messageText.takeAfter("@").toLong, messageText.takeBefore("@"))
                            case "INSTANT" => QrossTask.createInstantTask(queryId, messageText) match {
                                                case Some(task) => producer ! task
                                                case None =>
                                            }
                            case "KILL" =>
                                Try(messageText.toLong) match {
                                    case Success(actionId) => QrossTask.TO_BE_KILLED += actionId
                                    case _ =>
                                }
                            case _ =>
                        }
                    case "NOTE" =>
                        messageKey match {
                            case "PROCESS" =>
                                Try(messageText.toLong) match {
                                    case Success(noteId) => processor ! QrossNote.process(noteId, sender)
                                    case _ =>
                                }
                            case "KILL" =>
                                Try(messageText.toLong) match {
                                    case Success(noteId) => QrossNote.TO_BE_KILLED += noteId
                                    case _ =>
                                }
                            case _ =>
                        }
                    case "USER" =>
                        messageKey match {
                            case "MASTER" => Configurations.set("MASTER_USER_GROUP", QrossUser.getUsers("master"))
                            case "KEEPER" => Configurations.set("KEEPER_USER_GROUP", QrossUser.getUsers("keeper"))
                            case _ =>
                        }
                    case "PROPERTIES" =>
                        messageKey match {
                            //PROPERTIES - LOAD - path
                            case "LOAD" => Properties.loadLocalFile(messageText.locate())
                            case _ =>
                        }
                    case "CONNECTION" =>
                        //CONNECTION - UPSERT - connection_type.connection_name=connection_string#&#user_name#&#password
                        //CONNECTION - ENABLE/DISABLE/REMOVE - connection_name
                        messageKey match {
                            case "SETUP" => JDBC.setup(messageText.toInt)
                            case "REMOVE" => JDBC.remove(messageText)
                            case _ =>
                        }
                    case _ =>
                }
            }).clear()
        }
        while (Timer.rest() < nextMinute)
    }
}
