package io.qross.keeper

import java.io.File

import akka.actor.ActorSystem
import akka.http.scaladsl.model._
import akka.http.scaladsl.server
import akka.http.scaladsl.server.Directives._
import io.qross.core.DataHub
import io.qross.ext.Output
import io.qross.jdbc.JDBC
import io.qross.model.{Note, QrossNote, QrossTask, Route}
import io.qross.pql.{GlobalFunction, GlobalVariable}
import io.qross.setting.{Configurations, Global, Properties}
import io.qross.fs.TextFile._

import scala.util.Try


object Router {

    /*
    # PUT  /global/set?name=&value=
    # PUT /task/restart/taskId?more=
    # PUT /task/instant/jobId?dag=&params=&commands=&delay=&startTime=&creator=
    # PUT /task/kill/taskId
    PUT /note/process/noteid
    PUT /note/kill/noteId
    PUT /user/update/master
    PUT /user/update/keeper
    PUT /properties/load?path=
    PUT /connection/setup/id
    PUT /connection/enable/id
    PUT /connection/disable/id
    PUT /connection/remove/id
    */

    def rests(context: ActorSystem): server.Route = {

        val producer = context.actorSelection("akka://keeper/user/producer")
        val processor = context.actorSelection("akka://keeper/user/processor")

        pathSingleSlash {
            get {
                complete(HttpEntity(ContentTypes.`text/html(UTF-8)`, "<h1>Welcome to Qross Keeper!</h1>"))
            }
        } ~
            // PUT  /global/set?name=&value=
            path("global" / "set") {
                put {
                    parameters("name", "value") {
                        (name, value) => {
                            Output.writeLineWithSeal("SYSTEM", s"Update System Configuration '$name' to '$value'.")
                            Configurations.set(name, value)
                            complete(s"1")
                        }
                    }
                }
            } ~
            // PUT /task/restart/taskId?more=
            path("task" / "restart" / LongNumber) { taskId =>
                put {
                    parameter("more", "starter") {
                        (more, starter) => {
                            val task = QrossTask.restartTask(taskId, more, Try(starter.toInt).getOrElse(0))
                            producer ! task
                            complete(s"""{"id":${task.id},"status":"${task.status}","recordTime":"${task.recordTime}"}""")
                        }
                    }
                }
            } ~
            path("task" / "instant" / IntNumber) { jobId =>
                put {
                    parameter("creator".?[Int](0), "delay".?[Int](0), "ignore".?[String]("no")) {
                        (creator, delay, ignore) => {
                            val task = QrossTask.createInstantWholeTask(jobId, delay, ignore, creator)
                            producer ! task
                            complete(s"""{"id":${task.id},"recordTime":"${task.recordTime}"}""")
                        }
                    }
                }
            } ~
            path("task" / "instant") {
                put {
                    parameter("creator".as[Int]) {
                        creator => {
                            entity(as[String]) {
                                info => {
                                    val task = QrossTask.createInstantTask(info, creator)
                                    producer ! task
                                    complete(s"""{"id":${task.id},"recordTime":"${task.recordTime}"}""")
                                }
                            }
                        }
                    }
                }
            } ~
            path("job" / "kill" / IntNumber) { jobId =>
                put {
                    parameter("killer".as[Int]) {
                        killer => {
                            Output.writeDebugging(s"All tasks of job $jobId will be killed.")
                            val actions = Route.getRunningActionsOfJob(jobId)
                            actions.foreach(action => {
                                QrossTask.TO_BE_KILLED += action -> killer
                            })
                            complete(s"""{ "actions": [${actions.mkString(", ")}] }""")
                        }
                    }
                }
            } ~
            path("task" / "kill" / LongNumber) { taskId =>
                put {
                    parameter("killer".as[Int]) {
                        killer => {
                            Output.writeDebugging(s"All actions of task $taskId will be killed.")
                            val actions = Route.getRunningActionsOfTask(taskId)
                            actions.foreach(action => {
                                QrossTask.TO_BE_KILLED += action -> killer
                            })
                            complete(s"""{ "actions": [${actions.mkString(", ")}] }""")
                        }
                    }
                }
            } ~
            path("task" / "logs") {
                get {
                    parameter("jobId".as[Int], "taskId".as[Long], "recordTime".as[String],"cursor".?[Int](0), "actionId".?[Long](0), "mode".?[String]("all")) {
                        (jobId, taskId, recordTime, cursor, actionId, mode) => {
                            val datetime = new io.qross.time.DateTime(recordTime)
                            val path = s"""${Global.QROSS_HOME}/tasks/${datetime.getString("yyyyMMdd")}/$jobId/${taskId}_${datetime.getString("HHmmss")}.log"""
                            val file = new File(path)
                            if (file.exists()) {
                                val where = {
                                    if (mode == "debug") {
                                        if (actionId > 0) {
                                            s"WHERE actionId=$actionId AND logType<>'INFO'"
                                        }
                                        else {
                                            "WHERE logType<>'INFO'"
                                        }
                                    }
                                    else if (mode == "error") {
                                        if (actionId > 0) {
                                            s"WHERE actionId=$actionId AND logType='ERROR'"
                                        }
                                        else {
                                            "WHERE logType='ERROR'"
                                        }
                                    }
                                    else if (actionId > 0) {
                                        "WHERE actionId=" + actionId
                                    }
                                    else {
                                        ""
                                    }
                                }

                                val dh = DataHub.QROSS
                                dh.openJsonFile(path).asTable("logs")
                                val result = s"""{"logs": ${dh.executeDataTable(s"SELECT * FROM :logs SEEK $cursor $where LIMIT 100").toString}, "cursor": ${dh.cursor} }"""
                                dh.close()
                                complete(result)
                            }
                            else {
                                complete("""{"logs": [], cursor: -1, "error": "File not found."}""")
                            }
                        }
                    }
                }
            } ~
            path("action" / "kill" / LongNumber) { actionId =>
                put {
                    parameter("killer".as[Int]) {
                        killer => {
                            Route.getRunningAction(actionId) match {
                                case Some(_) =>
                                    Output.writeDebugging(s"Action $actionId will be killed.")
                                    QrossTask.TO_BE_KILLED += actionId -> killer
                                    complete(s"""{ "action": $actionId }""")
                                case None =>
                                    Output.writeDebugging(s"Action $actionId is not running.")
                                    complete(s"""{ "action": 0 }""")
                            }
                        }
                    }
                }
            } ~
            path("note" / "kill" / LongNumber) { noteId =>
                put {
                    if (Route.isNoteQuerying(noteId)) {
                        Output.writeDebugging(s"Note $noteId will be killed.")
                        QrossNote.TO_BE_STOPPED += noteId
                        complete(s"""{ "id": $noteId }""")
                    }
                    else {
                        Output.writeDebugging(s"Note $noteId is not running.")
                        complete(s"""{ "id": 0 }""")
                    }
                }
            } ~
            path ("note" / LongNumber) { noteId =>
                put {
                    parameter("user".as[Int]) {
                        user => {
                            processor ! Note(noteId, user)
                            complete(s"""{"id":$noteId}""")
                        }
                    }
                }
            }  ~
            path ("configurations" / "reload") {
                put {
                    Configurations.load()
                    complete("1")
                }
            } ~
            path ("properties" / "load" / IntNumber) { id =>
                put {
                    Properties.load(id)
                    complete("1")
                }
            } ~
            path ("connection" / "setup" / IntNumber) { id =>
                put {
                    JDBC.setup(id)
                    complete("1")
                }
                complete("1")
            } ~
            path ("connection" / "remove") {
                put {
                    parameter("connection_name") {
                        connectionName => {
                            JDBC.remove(connectionName)
                            complete("1")
                        }
                    }
                }
            } ~
            path ("function" / "renew") {
                put {
                    parameter("function_name") {
                        functionName => {
                            GlobalFunction.renew(functionName)
                            complete("1")
                        }
                    }
                }
            } ~
            path ("function" / "remove") {
                put {
                    parameter("function_name") {
                        functionName => {
                            GlobalFunction.remove(functionName)
                            complete("1")
                        }
                    }
                }
            } ~
            path ("variable" / "renew") {
                put {
                    parameter("variable_name") {
                        variableName => {
                            GlobalVariable.renew(variableName)
                            complete("1")
                        }
                    }
                }
            } ~
            path ("variable" / "remove") {
                put {
                    parameter("variable_name") {
                        variableName => {
                            GlobalVariable.remove(variableName)
                            complete("1")
                        }
                    }
                }
            } ~
            path ("test" / "json") {
                get {
                    parameter("id".as[Int], "name".as[String]) {
                        (id, name) =>
                            entity(as[String]) { json => {
                                complete(s"""[{"id":$id,"name":"$name"}, $json]""")
                            }
                        }
                    }
                }
            }
            /* 上条勿删
            REQUEST JSON API '''http://@KEEPER_HTTP_ADDRESS:@KEEPER_HTTP_PORT/test/json?id=1&name=Tom'''
                METHOD 'PUT'
                SEND DATA { "id": 2, "name": "Ted" };
            PARSE "/";
            */
            /* ~
            path("") {
                get {
                    complete(HttpEntity(ContentTypes.`application/json`,  Json.serialize(List[Int](1,2,3))))
                }
            } ~
            path("hello") {
                get{
                    complete(HttpEntity(ContentTypes.`text/html(UTF-8)`, "<h1>Say hello to akka-http</h1>"))
                }
            } ~
            (get & pathPrefix("hello" / LongNumber)) {
                complete("")
            } ~
            (get & pathPrefix("static")){
                getFromResourceDirectory("static")
            }

           path("auction") {
            concat(
              put {
                parameter("bid".as[Int], "user") { (bid, user) =>
                  // place a bid, fire-and-forget
                  auction ! Bid(user, bid)
                  complete((StatusCodes.Accepted, "bid placed"))
                }
              },
              get {
                implicit val timeout: Timeout = 5.seconds

                // query the actor for the current auction state
                val bids: Future[Bids] = (auction ? GetBids).mapTo[Bids]
                complete(bids)
              }
            )
          }
            */
    }
}