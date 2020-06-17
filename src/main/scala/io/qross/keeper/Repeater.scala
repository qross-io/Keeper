package io.qross.keeper

import io.qross.model.{QrossJob, QrossTask, Task, WorkActor}
import io.qross.time.{DateTime, Timer}

class Repeater extends WorkActor {

    private val starter = context.actorSelection("akka://keeper/user/starter")

    override def beat(tick: String): Unit = {

        QrossJob.refreshEndlessJobs()

        val nextMinute = new DateTime(tick).plusMinutes(1).toEpochSecond
        do {
            QrossJob.tickEndlessJobs()
                    .foreach(jobId => {
                        starter ! QrossTask.createEndlessTask(jobId)
                    })
        }
        while (Timer.rest() < nextMinute)
        //sleep to next second and return epoch second
    }
}
