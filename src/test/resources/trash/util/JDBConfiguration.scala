package io.qross.util

class JDBConfiguration(connectionName: String) {

    var connectionString: String = ""
    var driver: String = ""
    var username: String = ""
    var password: String = ""

    if (connectionName == "sqlite.memory") {
        this.connectionString = "jdbc:sqlite::memory:"
    }
    else if (connectionName.endsWith(".sqlite")) {
        this.connectionString = "jdbc:sqlite:" + connectionName
    }
    else if (Properties.contains(connectionName)) {
        this.connectionString = Properties.get(connectionName)
    }
    else if (Properties.contains(connectionName + ".url")) {
        this.connectionString = Properties.get(connectionName + ".url")
        this.username = Properties.get(connectionName + ".username")
        this.password = Properties.get(connectionName + ".password")
        this.driver = Properties.get(connectionName + ".driver")
    }
    else {
        throw new Exception("Can't find the connection name in properties.")
    }

    if (this.driver == "") {
        this.driver =   if (connectionName.contains("mysql") || connectionString.contains("mysql")) {
                            "com.mysql.cj.jdbc.Driver"
                        }
                        else if (connectionName.contains("sqlite") || connectionString.contains("sqlite")) {
                            "org.sqlite.JDBC"
                        }
                        else if (connectionName.contains("hive") || connectionString.contains("hive")) {
                            "org.apache.hive.jdbc.HiveDriver"
                        }
                        else if (connectionName.contains("presto") || connectionString.contains("presto")) {
                            "com.facebook.presto.jdbc.PrestoDriver"
                        }
                        else if (connectionName.contains("sqlserver") || connectionString.contains("sqlserver")) {
                            "com.microsoft.sqlserver.jdbc.SQLServerDriver"
                        }
                        else if (connectionName.contains("oracle") || connectionString.contains("oracle")) {
                            "oracle.jdbc.driver.OracleDriver"
                        }
                        else if (connectionName.contains("impala") || connectionString.contains("impala")) {
                            "org.apache.hive.jdbc.HiveDriver" //"com.cloudera.impala.jdbc4.Driver"
                        }
                        else {
                            ""
                        }
        if (driver == "") {
            throw new Exception("Can't match any driver to open database.")
        }
    }

    def isAlone: Boolean = {
        this.username != ""
    }
}
