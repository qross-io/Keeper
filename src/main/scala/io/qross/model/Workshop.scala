package io.qross.model

import io.qross.keeper.Setting
import io.qross.setting.Environment
import io.qross.time.Timer

object Workshop {

    val MAX: Int = Environment.cpuThreads * Setting.CONCURRENT_BY_CPU_CORES
    private var running: Int = 0

    def work(): Unit = synchronized {
        running += 1
    }

    def complete(): Unit = synchronized {
        running -= 1
    }

    def busy: Int = running
    def idle: Int = MAX - running

    def busyScore: Double = ((Environment.cpuUsage * 64 + running * 0.32 + Environment.systemMemoryUsage * 4) / (64 + MAX * 0.32 + 4) * 10000).round / 100d

    def delay(): Unit = Timer.sleep((busyScore * 10).round)
}