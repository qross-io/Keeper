package io.qross.util

import scala.collection.mutable.HashMap

object Common {

    def parseQueryString(queryString: String): HashMap[String, String] = {
        val params = queryString.split("&")
        val queries = new HashMap[String, String]()
        for (param <- params) {
            if (param.contains("=")) {
                queries += param.substring(0, param.indexOf("=")) -> param.substring(param.indexOf("=") + 1)
            }
            else {
                queries += param -> ""
            }
        }
        queries
    }
}
