package io.qross.keeper

import akka.actor.ActorSystem
import akka.http.scaladsl.model._
import akka.http.scaladsl.server
import akka.http.scaladsl.server.Directives._
import io.qross.ext.Output
import io.qross.model.{Note, QrossNote, QrossTask, Route}
import io.qross.net.Json
import io.qross.setting.Configurations

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
                    parameter("name", "value") {
                        (name, value) => {
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
                            complete(Json.serialize(task))
                        }
                    }
                }
            } ~
            //PUT /task/instant?more={}&creator=
            path("task" / "instant") {
                put {
                    parameter("info", "creator") {
                        (info, creator) => {
                                QrossTask.createInstantTask(info, Try(creator.toInt).getOrElse(0)) match {
                                    case Some(task) =>
                                        producer ! task
                                        complete(Json.serialize(task))
                                    case None =>
                                        complete("{ }")
                                }
                        }
                    }
                }
            } ~
            path("kill" / "job" / IntNumber) { jobId =>
                put {
                    parameter("killer".as[Int]) {
                        killer => {
                            Output.writeDebugging(s"All tasks of job $jobId will be killed.")
                            val actions = Route.getRunningActionsOfJob(jobId)
                            actions.foreach(action => {
                                QrossTask.TO_BE_KILLED += action -> killer
                            })
                            complete(s"""{ "actionsToBeKilled": [${actions.mkString(", ")}] }""")
                        }
                    }
                }
            } ~
            path("kill" / "task" / LongNumber) { taskId =>
                put {
                    parameter("killer".as[Int]) {
                        killer => {
                            Output.writeDebugging(s"All actions of task $taskId will be killed.")
                            val actions = Route.getRunningActionsOfTask(taskId)
                            actions.foreach(action => {
                                QrossTask.TO_BE_KILLED += action -> killer
                            })
                            complete(s"""{ "actionsToBeKilled": [${actions.mkString(", ")}] }""")
                        }
                    }
                }
            } ~
            path("kill" / "action" / LongNumber) { actionId =>
                put {
                    parameter("killer".as[Int]) {
                        killer => {
                            Route.getRunningAction(actionId) match {
                                case Some(_) =>
                                    Output.writeDebugging(s"Action $actionId will be killed.")
                                    QrossTask.TO_BE_KILLED += actionId -> killer
                                    complete(s"""{ "actionToBeKilled": $actionId }""")
                                case None =>
                                    Output.writeDebugging(s"Action $actionId is not running.")
                                    complete(s"""{ "actionToBeKilled": 0 }""")
                            }
                        }
                    }
                }
            } ~
            path("kill" / "note" / LongNumber) { noteId =>
                put {
                    if (Route.isNoteQuerying(noteId)) {
                        Output.writeDebugging(s"Note $noteId will be killed.")
                        QrossNote.TO_BE_KILLED += noteId
                        complete(s"""{ "noteToBeKilled": $noteId }""")
                    }
                    else {
                        Output.writeDebugging(s"Note $noteId is not running.")
                        complete(s"""{ "noteToBeKilled": 0 }""")
                    }
                }
            } ~
            path ("note" / LongNumber) { noteId =>
                put {
                    parameter("user".as[Int]) {
                        user => {
                            processor ! Note(noteId, user)
                            complete(s"$noteId")
                        }
                    }
                }
            } ~
            path ("user" / "group") {
                put {
                    Setting.renewUserGroup()
                    complete(s"""{ "keeper": "${Setting.KEEPER_USER_GROUP}", "master": "${Setting.MASTER_USER_GROUP}" }""")
                }
            }
        /*
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
