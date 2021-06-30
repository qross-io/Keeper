package io.qross.keeper

import akka.actor.ActorSystem
import akka.http.scaladsl.model._
import akka.http.scaladsl.server
import akka.http.scaladsl.server.Directives._
import io.qross.ext.Output
import io.qross.ext.TypeExt._
import io.qross.fs.Directory
import io.qross.jdbc.JDBC
import io.qross.model._
import io.qross.pql.{GlobalFunction, GlobalVariable}
import io.qross.setting.{Configurations, Global, Properties}


object Router {

    def rests(context: ActorSystem): server.Route = {

        val producer = context.actorSelection("akka://keeper/user/producer")
        val starter = context.actorSelection("akka://keeper/user/starter")
        val processor = context.actorSelection("akka://keeper/user/processor")

        pathSingleSlash {
            get {
                complete(HttpEntity(ContentTypes.`text/html(UTF-8)`, "<h1>Welcome to Qross Keeper!</h1>"))
            }
        } ~
            // PUT  /global/set?name=&value=
            path("global" / "set") {
                put {
                    parameter("name", "value", "terminus".?[String]("no")) {
                        (name, value, terminus) => {
                            Output.writeLineWithSeal("SYSTEM", s"Update System Configuration '$name' to '$value'.")
                            Configurations.set(name, value)
                            if (terminus == "no") {
                                Qross.distribute("PUT", s"global/set?name=$name&value=$value&")
                            }
                            complete("1")
                        }
                    }
                }
            } ~
            path("task" / "check" / LongNumber) { taskId =>
                put {
                    parameter("jobId".as[Int], "taskTime".as[String], "recordTime".as[String]) {
                        (jobId, taskTime, recordTime) => {
                            producer ! Task(taskId, TaskStatus.INITIALIZED).of(jobId).at(taskTime, recordTime)
                            complete("accept")
                        }
                    }
                }
            } ~
            path("task" / "start" / LongNumber) { taskId =>
                put {
                    parameter("jobId".as[Int], "taskTime".as[String], "recordTime".as[String], "terminus".?[String]("no")) {
                        (jobId, taskTime, recordTime, terminus) => {
                            if (Workshop.idle > 0 || terminus == "yes") {
                                starter ! Task(taskId, TaskStatus.READY).of(jobId).at(taskTime, recordTime)
                                complete("accept")
                            }
                            else {
                                complete("reject")
                            }
                        }
                    }

                }
            } ~
            // PUT /task/restart/taskId?more=
            path("task" / "restart" / LongNumber) { taskId =>
                put {
                    parameter("more".?[String]("WHOLE"), "starter".?[Int](0)) {
                        (more, starter) => {
                            val task = QrossTask.restartTask(taskId, more, starter)
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
                    parameter("killer".?[Int](0)) {
                        killer => {
                                complete(QrossJob.killJob(jobId, killer))
                        }
                    }
                }
            } ~
            path("task" / "kill" / LongNumber) { taskId =>
                put {
                    parameter("killer".?[Int](0), "recordTime".as[String]) {
                        (killer, recordTime) => {
                            complete(QrossTask.killTask(taskId, recordTime, killer))
                        }
                    }
                }
            } ~
            path("task" / "logs") {
                get {
                    parameter("jobId".as[Int], "taskId".as[Long], "recordTime".as[String],"cursor".?[Int](0), "actionId".?[Long](0), "mode".?[String]("all")) {
                        (jobId, taskId, recordTime, cursor, actionId, mode) => {
                            complete(QrossTask.getTaskLogs(jobId, taskId, recordTime, cursor, actionId, mode))
                        }
                    }
                }
            } ~
            path("job" / "logs" / IntNumber) { jobId =>
                delete {
                    parameter("terminus".?[String]("no")) {
                        terminus => {
                            QrossJob.deleteLogs(jobId)
                            if (terminus == "no") {
                                Qross.distribute("DELETE", s"job/logs/$jobId?")
                            }
                            complete("1")
                        }
                    }
                }
            } ~
            path("action" / "kill" / LongNumber) { actionId =>
                put {
                    parameter("killer".?[Int](0)) {
                        killer => {
                            complete(QrossTask.killAction(actionId, killer))
                        }
                    }
                }
            } ~
            path("note" / "kill" / LongNumber) { noteId =>
                put {
                    if (QrossNote.QUERYING.contains(noteId)) {
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
                    parameter("user".?[Int](0)) {
                        user => {
                            processor ! Note(noteId, user)
                            complete(s"""{"id": $noteId}""")
                        }
                    }
                }
            }  ~
            path ("configurations" / "reload") {
                put {
                    parameter("terminus".?[String]("no")) {
                        terminus => {
                            Configurations.load()
                            if (terminus == "no") {
                                Qross.distribute("PUT", s"configurations/reload?")
                            }
                            complete("1")
                        }
                    }
                }
            } ~
            path ("properties" / "load" / IntNumber) { id =>
                put {
                    parameter("terminus".?[String]("no")) {
                        terminus => {
                            Properties.load(id)
                            if (terminus == "no") {
                                Qross.distribute("PUT", s"properties/load/$id?")
                            }
                            complete("1")
                        }
                    }
                }
            } ~
            path ("connection" / IntNumber) { id =>
                put {
                    parameter("terminus".?[String]("no")) {
                        terminus => {
                            JDBC.setup(id)
                            if (terminus == "no") {
                                Qross.distribute("PUT", s"connection/$id?")
                            }
                            complete("1")
                        }
                    }
                }
            } ~
            path ("connection") {
                delete {
                    parameter("connectionName", "terminus".?[String]("no")) {
                        (connectionName, terminus) => {
                            JDBC.remove(connectionName)
                            if (terminus == "no") {
                                Qross.distribute("DELETE", s"connection?connectionName=$connectionName&")
                            }
                            complete("1")
                        }
                    }
                }
            } ~
            path ("function") {
                put {
                    parameter("functionName", "terminus".?[String]("no")) {
                        (functionName, terminus) => {
                            GlobalFunction.renew(functionName)
                            if (terminus == "no") {
                                Qross.distribute("PUT", s"function?functionName=$functionName&")
                            }
                            complete("1")
                        }
                    }
                }
            } ~
            path ("function") {
                delete {
                    parameter("functionName", "terminus".?[String]("no")) {
                        (functionName, terminus) => {
                            GlobalFunction.remove(functionName)
                            if (terminus == "no") {
                                Qross.distribute("DELETE", s"function?functionName=$functionName&")
                            }
                            complete("1")
                        }
                    }
                }
            } ~
            path ("variable") {
                put {
                    parameter("variableName", "terminus".?[String]("no")) {
                        (variableName, terminus) => {
                            GlobalVariable.renew(variableName)
                            if (terminus == "no") {
                                Qross.distribute("PUT", s"variable?variableName=$variableName&")
                            }
                            complete("1")
                        }
                    }
                }
            } ~
            path ("variable") {
                delete {
                    parameter("variableName", "terminus".?[String]("no")) {
                        (variableName, terminus) => {
                            GlobalVariable.remove(variableName)
                            if (terminus == "no") {
                                Qross.distribute("DELETE", s"variable?variableName=$variableName&")
                            }
                            complete("1")
                        }
                    }
                }
            } ~
            path("space" / "home") {
                get {
                    complete(Directory.spaceUsage(Global.QROSS_HOME).toHumanized)
                }
            } ~
            path("space" / "keeper-logs") {
                get {
                    complete(Directory.spaceUsage(s"${Global.QROSS_HOME}/keeper/logs").toHumanized)
                }
            } ~
            path("space" / "tasks") {
                get {
                    complete(Directory.spaceUsage(s"${Global.QROSS_HOME}/tasks").toHumanized)
                }
            } ~
            path ("test" / "json") {
                get {
                    parameter("id".as[Int], "name".?[String]("Tom"), "age".?("18")) {
                        (id, name, age) => {
                            entity(as[String]) {
                                json => {
                                    complete(HttpEntity(ContentTypes.`application/json`, s"""[{"id":$id,"name":"$name", "age": $age}, $json]"""))
                                }
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