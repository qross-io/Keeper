package io.qross.keeper

import io.qross.model._
import io.qross.util.{DateTime, Output, Timer}

class Messager extends WorkActor {
    
    private val producer = context.actorSelection("akka://keeper/user/producer")
    private val starter = context.actorSelection("akka://keeper/user/starter")
    
    override def beat(tick: String): Unit = {
        super.beat(tick)
        
        val nextMinute = DateTime(tick).plusMinutes(1).toEpochSecond
        do {
            MessageBox.check().foreach(row => {
                val messageType = row.getString("message_type").toUpperCase
                val messageKey = row.getString("message_key").toUpperCase
                val messageText = row.getString("message_text")
                Output.writeDebugging(s"Got A Message # $messageType # $messageKey # $messageText")
                messageType match {
                    case "GLOBAL" => Global.CONFIG.set(messageKey, messageText)
                    case "TASK" =>
                        //TASK - RESTART - WHOLE@TaskID - WHOLE@123
                        //TASK - RESTART - ^CommandIDs@TaskID - ^1,2,3,4,5@123
                        //TASK - RESTART - ^EXCEPTIONAL@TaskID - ^EXCEPTIONAL@123
                        //TASK - RESTART - CommandIDs@TaskID - 1,2,3,4,5@123
                        messageKey match {
                            case "RESTART" => producer ! Task(messageText.substring(messageText.indexOf("@") + 1).toLong, messageText.substring(0, messageText.indexOf("@")))
                            case _ =>
                        }
                    case "USER" =>
                        //USER - INSERT - role:name<email@doman.com>#password
                        //USER - UPDATE - name:name#id / mail:email@doman.com#id / role:role_name#id / password:password#id
                        //USER - DELETE - name:name / id:id
                        //USER - SELECT - only refresh keeper and master
                        messageKey match {
                            case "INSERT" => User.create(messageText)
                            case "UPDATE" => User.update(messageText)
                            case "DELETE" => User.remove(messageText)
                            case "SELECT" =>
                            case _ =>
                        }
                        Global.CONFIG.set("KEEPER_USER_GROUP", User.getUsers("keeper"))
                        Global.CONFIG.set("MASTER_USER_GROUP", User.getUsers("master"))
                    case "CONNECTION" =>
                        //CONNECTION - INSERT - connection_name=connection_string, user_name, password
                        //CONNECTION - UPDATE -
                        //CONNECTION - DELETE -
                        messageKey match {
                            case "INSERT" => JDBCConnection.create(messageText);
                            case "UPDATE" => JDBCConnection.update(messageText);
                            case "DELETE" => JDBCConnection.remove(messageText);
                            case _ =>
                        }
                    case _ =>
                }
            }).clear()
        }
        while (Timer.rest() < nextMinute)
    }
}
