package io.qross.util

import scala.collection.mutable

case class Parameter(private val SQL: String) {
    
    private val FIELDS = new mutable.HashMap[String, String]()
    private val SYMBOLS = List("\\#", "\\$") //, "@")
    
    SYMBOLS.foreach(symbol => {
        FIELDS ++= s"""$symbol\\{?([a-zA-Z_][a-zA-Z0-9_]+)\\}?""".r.findAllMatchIn(this.SQL).map(m => {
            (m.group(0), m.group(1))
        })
    })
    
    def matched: Boolean = FIELDS.nonEmpty
    
    def replaceWith(row: DataRow): String = {
        var replacement = this.SQL
        for ((placeHolder, fieldName) <- FIELDS) {
            if (placeHolder.startsWith("#")) {
                //#
                if (row.contains(fieldName)) {
                    replacement = replacement.replace(placeHolder, row.getString(fieldName))
                }
            }
            else {
                // $, @
                if (row.contains(fieldName)) {
                    replacement = replacement.replace(placeHolder, (row.getDataType(fieldName), row.get(fieldName)) match {
                        case (Some(dataType), Some(value)) =>
                            if (value == null) {
                                "NULL"
                            }
                            else if (dataType == DataType.INTEGER || dataType == DataType.DECIMAL) {
                                value.toString
                            }
                            else {
                                "'" + value.toString.replace("'", "''") + "'"
                            }
                        case _ => ""
                    })
                }
            }
        }
        
        replacement
    }
}