package io.qross.keeper

import io.qross.model._

class Inspector extends WorkActor {

    //private val producer = context.actorSelection("akka://keeper/user/producer")
    //private val processor = context.actorSelection("akka://keeper/user/processor")
    //private val starter = context.actorSelection("akka://keeper/user/starter")
    
    override def beat(tick: String): Unit = {
        //super.beat(tick)

        Qross.check(tick)

//        val nextMinute = new DateTime(tick).plusMinutes(1).toEpochSecond
//        do {
//            Timer.sleep(3000)
//        }
//        while (Timer.rest() < nextMinute)
    }
}
