package io.qross.util

object FilePath {
    
    def locate(path: String): String = {
        val full = path.replace("\\", "/")
        if (full.startsWith("/") || full.contains(":/")) {
            full
        }
        else {
            Properties.getDataExchangeDirectory + full
        }
    }
    
}
