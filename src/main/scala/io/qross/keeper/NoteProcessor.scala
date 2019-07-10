package io.qross.keeper

import akka.actor.Props
import akka.routing.BalancingPool
import io.qross.model.{Note, WorkActor}
import io.qross.setting.Global

class NoteProcessor extends WorkActor{

    private val performer = context.actorOf(Props[NotePerformer].withRouter(new BalancingPool(Global.CORES)), "performer")

    override def process(note: Note): Unit = {



    }
}