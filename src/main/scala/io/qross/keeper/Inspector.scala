package io.qross.keeper

import io.qross.model.Qross

object Inspector {
    def main(args: Array[String]): Unit = {
        Qross.checkBeatsAndRecords()
    }
}
