package io.qross.keeper

import akka.actor.{Actor, PoisonPill}
import io.qross.model.{Beats, TaskRecord, Tick, WorkActor}
import io.qross.util.DateTime

class TaskLogger extends WorkActor {
    
    override def beat(tick: String): Unit = {
        TaskRecord.saveAll()
        super.beat(tick)
    }
}
