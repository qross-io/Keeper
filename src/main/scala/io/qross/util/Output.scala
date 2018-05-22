package io.qross.util

object Output {
    
    def writeLine(messages: Any*): Unit = {
        for (message <- messages) {
            print(message)
        }
        println()
    }
    
    def writeDotLine(delimiter: String, messages: Any*): Unit = {
        for (i <- 0 until messages.length) {
            if (i > 0) print(delimiter)
            print(messages(i))
        }
        println()
    }
    
    def writeLines(messages: Any*): Unit = {
        for (message <- messages) {
            println(message)
        }
    }
    
    def writeMessage(messages: Any*): Unit = {
        for (message <- messages) {
            println(DateTime.now.getString("yyyy-MM-dd HH:mm:ss") + " [INFO] " + message)
        }
    }
    
    def writeExceptions(messages: Any*): Unit = {
        for (message <- messages) {
            System.err.println(DateTime.now.getString("yyyy-MM-dd HH:mm:ss") + " [ERROR] " + message)
        }
    }
}