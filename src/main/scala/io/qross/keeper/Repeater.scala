package io.qross.keeper

import io.qross.model.{EndlessJob, QrossTask, WorkActor, Workshop}
import io.qross.time.{DateTime, Timer}

class Repeater extends WorkActor {

    private val starter = context.actorSelection("akka://keeper/user/starter")

    override def beat(tick: String): Unit = {

        Workshop.delay()

        val locked = EndlessJob.refreshEndlessJobs(tick)
        if (locked) {
            val nextMinute = new DateTime(tick).plusMinutes(1).toEpochSecond
            do {
                EndlessJob.tickEndlessJobs()
                    .foreach(jobId => {
                        starter ! QrossTask.createEndlessTask(jobId)
                    })
            }
            while (Timer.rest() < nextMinute && !Setting.QUIT_ON_NEXT_BEAT)
        }
        //sleep to next second and return epoch second
    }
}
