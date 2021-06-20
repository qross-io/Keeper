package io.qross.keeper

import io.qross.ext.TypeExt._
import io.qross.setting.Configurations

object Setting {

    Configurations.set("QUIT_ON_NEXT_BEAT", false)

    def QUIT_ON_NEXT_BEAT: Boolean = Configurations.get("QUIT_ON_NEXT_BEAT").toBoolean(false)  //for keeper only

    def COMPANY_NAME: String = Configurations.getOrProperty("COMPANY_NAME", "company.name", "")

    def CONCURRENT_BY_CPU_CORES: Int = Configurations.getOrProperty("CONCURRENT_BY_CPU_CORES", "concurrent.by.cpu.cores").ifNullOrEmpty("4").toInt

    def BEAT_EVENTS_FIRE_FREQUENCY: String = Configurations.getOrProperty("BEAT_EVENTS_FIRE_FREQUENCY", "beat.events.fire.frequency")

    def KEEP_LOGS_FOR_X_DAYS: Int = Configurations.getOrProperty("KEEP_LOGS_FOR_X_DAYS", "keep.logs.for.x.days").toInt
}
