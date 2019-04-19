package io.qross.model

import io.qross.util.DataSource

object JDBCConnection {
    
    def upsert(connectionText: String): Unit = {
        val connection = JDBCConnection.parse(connectionText)
        val ds = new DataSource()
        if (!ds.executeExists("SELECT id FROM qross_connections WHERE connection_name=?", connection.connectionName)) {
            ds.executeNonQuery("INSERT INTO qross_connections (connection_type, connection_name, connection_string, username, password) VALUES (?, ?, ?, ?, ?)",
                connection.connectionType,
                connection.connectionName,
                connection.connectionString,
                connection.userName,
                connection.password
            )
        }
        else {
            ds.executeNonQuery("UPDATE qross_connections SET connection_type=?, connection_string=?, username=?, password=? WHERE connection_name=?",
                connection.connectionType,
                connection.connectionString,
                connection.userName,
                connection.password,
                connection.connectionName
            )
        }
        ds.close()
    }
    
    def enable(connectionName: String): Unit = {
        DataSource.queryUpdate("UPDATE qross_connections SET enabled='yes' WHERE connection_name=?", connectionName)
    }
    
    def disable(connectionName: String): Unit = {
        DataSource.queryUpdate("UPDATE qross_connections SET enabled='no' WHERE connection_name=?", connectionName)
    }
    
    def remove(connectionName: String): Unit = {
        DataSource.queryUpdate("DELETE FROM qross_connections WHERE connection_name=?", connectionName)
    }
    
    def parse(connection: String): JDBCConnection = {
        //connectionType.connectionName=url#&#userNme#&#password
        val delimiter = "#&#"
        
        val connectionType = connection.substring(0, connection.indexOf("."))
        val connectionName = connection.substring(connection.indexOf(".") + 1, connection.indexOf("="))
        var connectionString = connection.substring(connection.indexOf("=") + 1)
        var userName = ""
        var password = ""
        if (connectionString.contains(delimiter)) {
            userName = connectionString.substring(connectionString.indexOf(delimiter) + 3)
            password = userName.substring(connectionString.indexOf(delimiter) + 3)
            userName = userName.substring(0, userName.indexOf(delimiter))
            connectionString = connectionString.substring(0, connectionString.indexOf(delimiter))
        }
        
        new JDBCConnection(connectionType, connectionName, connectionString, userName, password)
    }
}

class JDBCConnection(val connectionType: String, val connectionName: String, val connectionString: String, val userName: String = "", val password: String = "") {

}
