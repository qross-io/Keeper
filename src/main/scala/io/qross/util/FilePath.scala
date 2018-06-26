package io.qross.util

import io.qross.model.Global

object FilePath {
    
    def locate(path: String): String = {
        val full = path.replace("\\", "/")
        if (full.startsWith("/") || full.contains(":/")) {
            full
        }
        else {
            Global.QROSS_KEEPER_HOME + full
        }
    }
    
    def format(path: String): String = {
        var dir = path.replace("\\", "/")
        if (!path.endsWith("/")) {
            dir += "/"
        }
        dir
    }
    
    def parse(path: String): String = {
        var dir = path.replace("\\", "/")
        dir.replace("%USER_HOME", Global.USER_HOME)
            .replace("%JAVA_BIN_HOME", Global.JAVA_BIN_HOME)
            .replace("%QROSS_HOME", Global.QROSS_HOME)
            .replace("%QROSS_KEEPER_HOME", Global.QROSS_KEEPER_HOME)
            .replace("%QROSS_WORKER_HOME", Global.QROSS_WORKER_HOME)
            .replace("//", "/")
    }
}
