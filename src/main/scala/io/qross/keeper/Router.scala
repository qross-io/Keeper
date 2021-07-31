package io.qross.keeper

import akka.actor.ActorSystem
import akka.http.scaladsl.model._
import akka.http.scaladsl.server
import akka.http.scaladsl.server.Directives.{complete, _}
import akka.http.scaladsl.server.StandardRoute
import io.qross.ext.Output
import io.qross.ext.TypeExt._
import io.qross.fs.Directory
import io.qross.jdbc.JDBC
import io.qross.model._
import io.qross.pql.{GlobalFunction, GlobalVariable}
import io.qross.setting.{Configurations, Global, Properties}

object Router {

    def auth(token: String)(callback: () => StandardRoute): StandardRoute = {
        if (token == Global.KEEPER_HTTP_TOKEN) {
            callback()
        }
        else {
            complete("Access Denied.")
        }
    }

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
                parameter("name", "value", "terminus".?[String]("no"), "token") {
                    (name, value, terminus, token) => {
                        auth(token) { () =>
                            Output.writeLineWithSeal("SYSTEM", s"Update System Configuration '$name' to '$value'.")
                            Configurations.set(name, value)
                            if (terminus == "no") {
                                Qross.distribute("PUT", s"global/set?name=$name&value=$value&")
                            }
                            complete("1")
                        }
                    }
                }
            }
        } ~
        path("properties") {
            get {
                parameter("name", "token") {
                    (name, token) => {
                        auth(token) { () =>
                            complete(name)
                        }
                    }
                }
            }
        } ~
        path("keeper" / "logs") {
            get {
                parameter("hour".?[String](io.qross.time.DateTime.now.getString("HH")), "cursor".?[Int](0), "token") {
                    (hour, cursor, token) => {
                        auth(token) { () =>
                            complete(Qross.getKeeperLogs(hour, cursor))
                        }
                    }
                }
            }
        } ~
        path("task" / "check" / LongNumber) { taskId =>
            put {
                parameter("jobId".as[Int], "taskTime".as[String], "recordTime".as[String], "token") {
                    (jobId, taskTime, recordTime, token) => {
                        auth(token) { () =>
                            producer ! Task(taskId, TaskStatus.INITIALIZED).of(jobId).at(taskTime, recordTime)
                            complete("accept")
                        }
                    }
                }
            }
        } ~
        path("task" / "start" / LongNumber) { taskId =>
            put {
                parameter("jobId".as[Int], "taskTime".as[String], "recordTime".as[String], "terminus".?[String]("no"), "token") {
                    (jobId, taskTime, recordTime, terminus, token) => {
                        auth(token) { () =>
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
            }
        } ~
        // PUT /task/restart/taskId?more=
        path("task" / "restart" / LongNumber) { taskId =>
            put {
                parameter("option".?[String]("WHOLE"), "starter".?[Int](0), "token") {
                    (option, starter, token) => {
                        auth(token) { () =>
                            val task = QrossTask.restartTask(taskId, option, starter)
                            producer ! task
                            complete(s"""{"id":${task.id},"status":"${task.status}","recordTime":"${task.recordTime}"}""")
                        }
                    }
                }
            }
        } ~
        path("task" / "restart") {
          put {
              parameter("taskIds".as[String], "option".?[String]("WHOLE"), "starter".?[Int](0), "token") {
                  (taskIds, option, starter, token) => {
                      auth(token) { () =>
                          complete(QrossTask.restartAll(taskIds, option, starter, producer))
                      }
                  }
              }
          }
        } ~
        path("task" / "instant" / IntNumber) { jobId =>
          put {
              parameter("creator".?[Int](0), "delay".?[Int](0), "ignore".?[String]("no"), "token") {
                  (creator, delay, ignore, token) => {
                      auth(token) { () =>
                          val task = QrossTask.createInstantWholeTask(jobId, delay, ignore, creator)
                          producer ! task
                          complete(s"""{"id":${task.id},"recordTime":"${task.recordTime}"}""")
                      }
                  }
              }
          }
        } ~
        path("task" / "instant") {
          put {
              parameter("creator".as[Int], "token") {
                  (creator, token) => {
                      entity(as[String]) {
                          info => {
                              auth(token) { () =>
                                  val task = QrossTask.createInstantTask(info, creator)
                                  producer ! task
                                  complete(s"""{"id":${task.id},"recordTime":"${task.recordTime}"}""")
                              }
                          }
                      }
                  }
              }
          }
        } ~
        path("job" / "kill" / IntNumber) { jobId =>
            put {
                parameter("killer".?[Int](0), "token") {
                    (killer, token) => {
                        auth(token) { () =>
                            complete(QrossJob.killJob(jobId, killer))
                        }
                    }
                }
            }
        } ~
        path("job" / "restart" / IntNumber) { jobId =>
          put {
              parameter("taskId".as[Long], "taskTime".as[String], "option".?[String]("WHOLE"), "starter".?[Int](0), "excluding".?[String]("0"), "token") {
                  (taskId, taskTime, option, starter, excluding, token) => {
                      auth(token) { () =>
                          val taskIds = QrossTask.restartJobFlow(jobId, taskId, taskTime, excluding.split(",").map(_.toInt): _*)
                          complete(QrossTask.restartAll(taskIds, option, starter, producer))
                      }
                  }
              }
          }
        } ~
        path("task" / "kill" / LongNumber) { taskId =>
          put {
              parameter("killer".?[Int](0), "recordTime".as[String], "token") {
                  (killer, recordTime, token) => {
                      auth(token) { () =>
                          complete(QrossTask.killTask(taskId, recordTime, killer))
                      }
                  }
              }
          }
        } ~
        path("task" / "logs") {
            get {
                parameter("jobId".as[Int], "taskId".as[Long], "recordTime".as[String], "cursor".?[Int](0), "actionId".?[Long](0), "mode".?[String]("all"), "token") {
                    (jobId, taskId, recordTime, cursor, actionId, mode, token) => {
                        auth(token) { () =>
                            complete(QrossTask.getTaskLogs(jobId, taskId, recordTime, cursor, actionId, mode))
                        }
                    }
                }
            }
        } ~
        path("job" / "logs" / IntNumber) { jobId =>
            delete {
                parameter("terminus".?[String]("no"), "token") {
                    (terminus, token) => {
                        auth(token) { () =>
                            QrossJob.deleteLogs(jobId)
                            if (terminus == "no") {
                                Qross.distribute("DELETE", s"job/logs/$jobId?")
                            }
                            complete("1")
                        }
                    }
                }
            }
        } ~
        path("action" / "kill" / LongNumber) { actionId =>
            put {
                parameter("killer".?[Int](0), "token") {
                    (killer, token) => {
                        auth(token) { () =>
                            complete(QrossTask.killAction(actionId, killer))
                        }
                    }
                }
            }
        } ~
        path("note" / "kill" / LongNumber) { noteId =>
            put {
                parameter("token") {
                    token => {
                        auth(token) { () =>
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
                    }
                }
            }
        } ~
        path("note" / LongNumber) { noteId =>
            put {
              parameter("user".?[Int](0), "token") {
                  (user, token) => {
                      auth(token) { () =>
                          processor ! Note(noteId, user)
                          complete(s"""{"id": $noteId}""")
                      }
                  }
              }
            }
        } ~
        path("configurations" / "reload") {
          put {
              parameter("terminus".?[String]("no"), "token") {
                  (terminus, token) => {
                      auth(token) { () =>
                          Configurations.load()
                          if (terminus == "no") {
                              Qross.distribute("PUT", s"configurations/reload?")
                          }
                          complete("1")
                      }
                  }
              }
          }
        } ~
        path("properties" / "load" / IntNumber) { id =>
          put {
              parameter("terminus".?[String]("no"), "token") {
                  (terminus, token) => {
                      auth(token) { () =>
                          Properties.load(id)
                          if (terminus == "no") {
                              Qross.distribute("PUT", s"properties/load/$id?")
                          }
                          complete("1")
                      }
                  }
              }
          }
        } ~
        path("connection" / IntNumber) { id =>
          put {
              parameter("terminus".?[String]("no"), "token") {
                  (terminus, token) => {
                      auth(token) { () =>
                          JDBC.setup(id)
                          if (terminus == "no") {
                              Qross.distribute("PUT", s"connection/$id?")
                          }
                          complete("1")
                      }
                  }
              }
          }
        } ~
        path("connection") {
          delete {
              parameter("connectionName", "terminus".?[String]("no"), "token") {
                  (connectionName, terminus, token) => {
                      auth(token) { () =>
                          JDBC.remove(connectionName)
                          if (terminus == "no") {
                              Qross.distribute("DELETE", s"connection?connectionName=$connectionName&")
                          }
                          complete("1")
                      }
                  }
              }
          }
        } ~
        path("function") {
          put {
              parameter("functionName", "terminus".?[String]("no"), "token") {
                  (functionName, terminus, token) => {
                      auth(token) { () =>
                          GlobalFunction.renew(functionName)
                          if (terminus == "no") {
                              Qross.distribute("PUT", s"function?functionName=$functionName&")
                          }
                          complete("1")
                      }
                  }
              }
          }
        } ~
        path("function") {
          delete {
              parameter("functionName", "terminus".?[String]("no"), "token") {
                  (functionName, terminus, token) => {
                      auth(token) { () =>
                          GlobalFunction.remove(functionName)
                          if (terminus == "no") {
                              Qross.distribute("DELETE", s"function?functionName=$functionName&")
                          }
                          complete("1")
                      }
                  }
              }
          }
        } ~
        path("variable") {
          put {
              parameter("variableName", "terminus".?[String]("no"), "token") {
                  (variableName, terminus, token) => {
                      auth(token) { () =>
                          GlobalVariable.renew(variableName)
                          if (terminus == "no") {
                              Qross.distribute("PUT", s"variable?variableName=$variableName&")
                          }
                          complete("1")
                      }
                  }
              }
          }
        } ~
        path("variable") {
          delete {
              parameter("variableName", "terminus".?[String]("no"), "token") {
                  (variableName, terminus, token) => {
                      auth(token) { () =>
                          GlobalVariable.remove(variableName)
                          if (terminus == "no") {
                              Qross.distribute("DELETE", s"variable?variableName=$variableName&")
                          }
                          complete("1")
                      }
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
        path("test" / "json") {
            get {
                parameter("id".as[Int], "name".?[String]("Tom"), "time".?("1978-04-27 12:30:00")) {
                    (id, name, time) => {
                        entity(as[String]) {
                            json => {
                                complete(HttpEntity(ContentTypes.`application/json`, s"""[{"id":$id,"name":"$name", "time": "$time"}, $json]"""))
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