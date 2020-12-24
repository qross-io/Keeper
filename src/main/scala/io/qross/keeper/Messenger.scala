package io.qross.keeper

import io.qross.ext.Output
import io.qross.ext.TypeExt._
import io.qross.fs.Path._
import io.qross.jdbc.JDBC
import io.qross.model._
import io.qross.setting.{Configurations, Properties}
import io.qross.time.{DateTime, Timer}

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
                val sender = row.getInt("sender", 0)
                val messageType = row.getString("message_type").toUpperCase
                val messageKey = row.getString("message_key").toUpperCase
                val messageText = row.getString("message_text")
                Output.writeLineWithSeal("SYSTEM", s"Got a Message # $queryId # $messageType # $messageKey # $messageText")

                messageType match {
                    case "GLOBAL" =>
                        if (messageKey == "USER_GROUP") {

                        }
                        else {
                            Configurations.set(messageKey, messageText)
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
                                start_time: 'yyyy-MM-dd HH:mm:00'
                            }
                            */
                        //TASK - KILL - actionId
                        messageKey match {
                            case "RESTART" => producer ! QrossTask.restartTask(messageText.takeAfter("@").toLong, messageText.takeBefore("@"), sender)
                            case "INSTANT" => producer ! QrossTask.createInstantTask(messageText, sender, queryId)
                            case "KILL" =>
                                Try(messageText.toLong) match {
                                    case Success(actionId) => QrossTask.TO_BE_KILLED += actionId -> sender
                                    case _ =>
                                }
                            case _ =>
                        }
                    case "NOTE" =>
                        messageKey match {
                            case "PROCESS" =>
                                Try(messageText.toLong) match {
                                    case Success(noteId) => processor ! Note(noteId, sender)
                                    case _ =>
                                }
                            case "STOP" =>
                                Try(messageText.toLong) match {
                                    case Success(noteId) => QrossNote.TO_BE_STOPPED += noteId
                                    case _ =>
                                }
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
