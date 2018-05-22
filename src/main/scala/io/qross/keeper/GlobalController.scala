package io.qross.keeper

import io.qross.model.{Global, WorkActor}
import io.qross.util.DateTime

class GlobalController extends WorkActor {
    override def beat(tick: String): Unit = {
        Global.runSystemTasks(tick)
    }
}
