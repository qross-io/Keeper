package io.qross.keeper

import io.qross.ext.TypeExt._
import io.qross.jdbc.{DataSource, JDBC}
import io.qross.setting.Configurations

object Setting {

    Configurations.set("QUIT_ON_NEXT_BEAT", false)

    def QUIT_ON_NEXT_BEAT: Boolean = Configurations.get("QUIT_ON_NEXT_BEAT").toBoolean(false)  //for keeper only

    def COMPANY_NAME: String = Configurations.getOrProperty("COMPANY_NAME", "company.name", "")

    def CONCURRENT_BY_CPU_CORES: Int = Configurations.getOrProperty("CONCURRENT_BY_CPU_CORES", "concurrent.by.cpu.cores").ifNullOrEmpty("4").toInt

    def EMAIL_EXCEPTIONS_TO_DEVELOPER: Boolean = Configurations.getOrProperty("EMAIL_EXCEPTIONS_TO_DEVELOPER", "email.exceptions.to.developer").toBoolean(true)

    def BEATS_MAILING_FREQUENCY: String = Configurations.getOrProperty("BEATS_MAILING_FREQUENCY", "beats.mailing.frequency")
}
