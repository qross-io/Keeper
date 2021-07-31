package io.qross.model

import io.qross.keeper.Setting
import io.qross.setting.Environment
import io.qross.time.Timer

import scala.util.Random

object Workshop {

    val MAX: Int = Environment.cpuThreads * Setting.CONCURRENT_BY_CPU_CORES
    private var running: Int = 0

    def work(): Unit = synchronized {
        running += 1
    }

    def complete(): Unit = synchronized {
        if (running > 0) {
            running -= 1
        }
        else {
            0
        }
    }

    def busy: Int = running
    def idle: Int = MAX - running

    def busyScore: Double = ((Environment.cpuUsage.abs * 64 + running.abs * 0.32 + Environment.systemMemoryUsage.abs * 4) / (64 + MAX * 0.32 + 4) * 10000d).round / 100d

    def delay(): Unit = {
         Timer.sleep((busyScore * (Random.nextInt(3) + 1)).round.abs)
    }
}