package io.qross.model

import io.qross.util.DataSource

object QrossUser {
    
    //USER - INSERT - role:name<email@doman.com>#password
    //USER - UPDATE - username:name#id / email:email@doman.com#id / role:role_name#id / password:password#id
    //USER - DELETE - id

    // ## Remove create, update, remove method at 8.27
    //USER - INSERT - role:name<email@doman.com>#password
    //USER - UPDATE - name:name#id / mail:email@doman.com#id / role:role_name#id / password:password#id
    //USER - DELETE - name:name / id:id
    //USER - SELECT - only refresh keeper and master

    /*
    def create(command: String): Unit = {
        val role = command.substring(0, command.indexOf(":")).toLowerCase()
        val username = command.substring(command.indexOf(":") + 1, command.indexOf("<"))
        val email = command.substring(command.indexOf("<") + 1, command.lastIndexOf(">"))
        val password = command.substring(command.lastIndexOf("#") + 1)
    
        DataSource.queryUpdate("INSERT INTO qross_users (role, username, email, password) VALUES (?, ?, ?, ?)", role, username, email, password)
    }
    
    def update(command: String): Unit = {
        val field = command.substring(0, command.indexOf(":"))
        val value = command.substring(command.indexOf(":") + 1, command.lastIndexOf("#"))
        val id = command.substring(command.lastIndexOf("#") + 1)
        
        DataSource.queryUpdate(s"UPDATE qross_users SET $field=? WHERE id=$id", value)
    }


    def remove(command: String): Unit = {
        DataSource.queryUpdate("DELETE FROM qross_users WHERE id=?", command)
    }
    */

    // Get users by role
    def getUsers(role: String): String = {
        DataSource.queryDataTable(s"SELECT GROUP_CONCAT(CONCAT(username, '<', email, '>') SEPARATOR ';') AS users FROM qross_users WHERE role='$role'")
    }
}
