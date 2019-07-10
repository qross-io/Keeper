package io.qross.util

import scala.collection.mutable.HashMap

object Common {

    def parseMapString(queryString: String, delimiter: String = "&", terminator: String = "="): HashMap[String, String] = {
        val params = queryString.split(delimiter)
        val queries = new HashMap[String, String]()
        for (param <- params) {
            if (param.contains(terminator)) {
                queries += param.substring(0, param.indexOf(terminator)) -> param.substring(param.indexOf(terminator) + 1)
            }
            else {
                queries += param -> ""
            }
        }
        queries
    }
}
