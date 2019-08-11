package io.qross.model

import io.qross.jdbc.DataSource

object QrossUser {
    
    // Get users by role
    def getUsers(role: String): String = {
        DataSource.QROSS.querySingleValue(s"SELECT GROUP_CONCAT(CONCAT(username, '<', email, '>') SEPARATOR ';') AS users FROM qross_users WHERE role='$role'").asText("")
    }
}
