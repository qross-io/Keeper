package io.qross.keeper

import io.qross.model.{TaskRecord, WorkActor}

class TaskLogger extends WorkActor {
    
    override def beat(tick: String): Unit = {
        TaskRecord.saveAll()
        super.beat(tick)
    }
    
    override def cleanup(): Unit = {
        TaskRecord.saveAll()
    }
}
