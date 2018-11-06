package io.qross.keeper

import io.qross.model.{TaskRecorder, WorkActor}

class TaskLogger extends WorkActor {
    
    override def beat(tick: String): Unit = {
        TaskRecorder.saveAll()
        super.beat(tick)
    }
    
    override def cleanup(): Unit = {
        TaskRecorder.saveAll()
    }
}
