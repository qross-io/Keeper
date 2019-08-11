package io.qross.model

import io.qross.core.DataTable
import io.qross.ext.Output._
import io.qross.jdbc.DataSource

object Beats {
    
    def beat(actor: String): Int = {
        writeMessage(actor + " beat!")
        DataSource.QROSS.queryUpdate(s"UPDATE qross_keeper_beats SET last_beat_time=NOW() WHERE actor_name='$actor'")
    }
    
    def start(actor: String, message: String = ""): Int = {
        writeDebugging(actor + " start! " + message)
        DataSource.QROSS.queryUpdate(s"UPDATE qross_keeper_beats SET status='running',start_time=NOW() WHERE actor_name='$actor'")
    }
    
    def quit(actor: String): Int = {
        writeDebugging(s"$actor quit!")
        DataSource.QROSS.queryUpdate(s"UPDATE qross_keeper_beats SET status='rest',quit_time=NOW() WHERE actor_name='$actor'")
    }
    
    def toHtml(table: DataTable): String = {
        val str = new StringBuilder()
        table.foreach(row => {
            str.append(row.getString("actor_name") + " - " +  row.getString("status") + " - " + row.getString("last_beat_time") + "<br/>")
        }).clear()
        str.toString()
    }
}
