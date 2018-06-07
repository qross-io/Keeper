package io.qross.keeper

import io.qross.model.Global

object Cleaner {
    def main(args: Array[String]): Unit = {
        Global.clearTaskRecords()
    }
}
