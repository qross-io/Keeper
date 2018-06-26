package io.qross.keeper

import io.qross.model.Global
import io.qross.util.DateTime

object Notifier {
    def main(args: Array[String]): Unit = {
        Global.checkBeats()
    }
}
