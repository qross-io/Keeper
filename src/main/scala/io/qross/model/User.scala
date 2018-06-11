package io.qross.model

import io.qross.util.DataSource

object User {
    
    //USER - INSERT - role:name<email@doman.com>#password
    //USER - UPDATE - username:name#id / email:email@doman.com#id / role:role_name#id / password:password#id
    //USER - DELETE - id
   
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
    
    def getUsers(role: String): String = {
        DataSource.queryDataTable(s"SELECT CONCAT(username, '<', email, '>') AS user FROM qross_users WHERE role='$role'").mkString(";", "user")
    }
}
