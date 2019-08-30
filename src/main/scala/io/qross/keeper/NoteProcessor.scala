package io.qross.keeper

import akka.actor.Props
import akka.routing.BalancingPool
import io.qross.model.{WorkActor, Process}
import io.qross.setting.Environment

class NoteProcessor extends WorkActor{

    private val performer = context.actorOf(Props[NotePerformer].withRouter(new BalancingPool(Environment.cpuThreads)), "performer")

    override def process(process: Process): Unit = {
        //distribute Process
        performer ! process
    }
}