package io.qross.util

import io.qross.model.Global.{CONFIG, USER_HOME}

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
    
    def format(path: String): String = {
        var dir = path.replace("\\", "/")
        if (!path.endsWith("/") && !path.endsWith("\\")) {
            dir += "/"
        }
        dir
    }
}
