package io.qross.keeper


import io.qross.model.Global
import io.qross.util.Properties

object Cleaner {
    def main(args: Array[String]): Unit = {
        Global.clearTaskRecords()
    }
}
