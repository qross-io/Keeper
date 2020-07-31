package io.qross.keeper

import io.qross.model.{TaskRecorder, WorkActor}
import io.qross.time.{DateTime, Timer}

class TaskLogger extends WorkActor {

    override def beat(tick: String): Unit = {

        super.beat(actorName)

        val nextMinute = new DateTime(tick).plusMinutes(1).toEpochSecond
        do {
            TaskRecorder.save()
        }
        while(Timer.rest() < nextMinute)
    }
    
    override def cleanup(): Unit = {
        TaskRecorder.dispose()
    }
}
