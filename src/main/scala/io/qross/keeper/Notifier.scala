package io.qross.keeper

import io.qross.model.Qross

object Notifier {
    def main(args: Array[String]): Unit = {
        Qross.checkBeats()
    }
}
