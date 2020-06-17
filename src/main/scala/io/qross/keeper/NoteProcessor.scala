package io.qross.keeper

import akka.actor.Props
import akka.routing.BalancingPool
import io.qross.model.{Note, Tick, WorkActor}
import io.qross.setting.Environment

class NoteProcessor extends WorkActor {

    private val querier = context.actorOf(Props[NoteQuerier].withRouter(new BalancingPool(Environment.cpuThreads)), "querier")

    override def process(noteId: Long, userId: Int): Unit = {
        //distribute Process
        querier ! Note(noteId, userId)
    }

    override def beat(minute: String): Unit = {
        querier ! Tick(minute)
        super.beat(minute)
    }
}