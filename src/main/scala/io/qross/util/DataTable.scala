package io.qross.util

import io.qross.util.Output._
import io.qross.util.DataType.DataType
import collection.JavaConverters._
import scala.collection.mutable
import scala.util.control.Breaks._

object DataTable {
    
    def from(dataTable: DataTable): DataTable = {
        DataTable().copy(dataTable)
    }
    
    def ofSchema(dataTable: DataTable): DataTable = {
        val table = DataTable()
        table.fields ++= dataTable.fields
        table
    }
    
    def withFields(fields: (String, DataType)*): DataTable = {
        val table = DataTable()
        fields.foreach(field => {
            table.addField(field._1, field._2)
        })
        table
    }
    
    def main(args: Array[String]): Unit = {
        val table = DataTable()
        table.insertRow("a" -> 1, "b" -> "HELLO", "c" -> "I", "d" -> "am", "e" -> "chinese", "f" -> "!")
        table.label(
            "a" -> "AA",
            "b" -> "BB",
            "c" -> "CC",
            "d" -> "DD",
            "e" -> "EE",
            "f" -> "FF"
        )
        table.show(10)
        
        println(table.toHtmlString)
        table.clear()
    }
}

case class DataTable(private val items: DataRow*) {
    
    private val rows = new mutable.ArrayBuffer[DataRow]()
    private val fields = new mutable.LinkedHashMap[String, DataType]()
    private val labels = new mutable.LinkedHashMap[String, String]()
    
    //initial rows
    for (row <- items) {
        this.addRow(row)
    }
    
    def addField(field: String, dataType: DataType): Unit = {
        // . is illegal char in SQLite
        val name = if (field.contains(".")) field.substring(field.lastIndexOf(".") + 1) else field
        this.fields += name -> dataType
        this.labels += name -> name
        //add column for every row
        //for (row <- this.rows) {
        //    row.set(name, null)
        //}
    }
    
    def label(alias: (String, String)*): DataTable = {
        for ((fieldName, otherName) <- alias) {
            this.labels += fieldName -> otherName
        }
        this
    }
    
    def contains(field: String): Boolean = this.fields.contains(field)
    
    def addRow(row: DataRow): Unit = {
        for (field <- row.getFields) {
            if (!this.contains(field)) {
                row.getDataType(field) match {
                    case Some(dataType) => this.addField(field, dataType)
                    case None =>
                }
            }
        }
        this.rows += row
    }
    
    def foreach(callback: DataRow => Unit): DataTable = {
        //val table = DataTable()
        this.rows.foreach(row => {
            callback(row)
            //table.addRow(row)
        })
        this //table
    }
    
    def collect(filter: DataRow => Boolean) (map: DataRow => DataRow): DataTable = {
        val table = DataTable()
        this.rows.foreach(row => {
            if (filter(row)) {
                table.addRow(map(row))
            }
        })
        table
    }
    
    def map(callback: DataRow => DataRow): DataTable = {
        val table = DataTable()
        this.rows.foreach(row => {
            table.addRow(callback(row))
        })
        table
    }
    
    def table(fields: (String, DataType)*)(callback: DataRow => DataTable): DataTable = {
        val table = DataTable.withFields(fields: _*)
        this.rows.foreach(row => {
            table.merge(callback(row))
        })
        table
    }
    
    def filter(callback: DataRow => Boolean): DataTable = {
        val table = DataTable()
        this.rows.foreach(row => {
            if (callback(row)) {
                table.addRow(row)
            }
        })
        table
    }
    
    def distinct(fieldNames: String*): DataTable = {
        val table = DataTable()
        val set = new mutable.HashSet[DataRow]()
        if (fieldNames.isEmpty) {
            this.rows.foreach(row => set += row)
        }
        else {
            this.rows.foreach(row => {
                val newRow = DataRow()
                fieldNames.foreach(fieldName => newRow.set(fieldName, row.get(fieldName).get))
                set += newRow
            })
        }
        set.foreach(row => table.addRow(row))
        
        table
    }
    
    def count(): Int = this.rows.size
    
    def count(groupBy: String*): DataTable = {
        val table = DataTable()
        if (groupBy.isEmpty) {
            table.addRow(DataRow("_count" -> this.count))
        }
        else {
            val map = new mutable.HashMap[DataRow, Int]()
            this.rows.foreach(row => {
                val newRow = DataRow.from(row, groupBy: _*)
                if (map.contains(newRow)) {
                    map.update(newRow, map(newRow) + 1)
                }
                else {
                    map.put(newRow, 1)
                }
            })
            for ((row, c) <- map) {
                row.set("_count", c)
                table.addRow(row)
            }
            map.clear()
        }
        table
    }
    
    def sum(fieldName: String, groupBy: String*): DataTable = {
        val table = DataTable()
        if (groupBy.isEmpty) {
            var s = 0D
            this.rows.foreach(row => s += row.getDoubleOption(fieldName).getOrElse(0D))
            table.addRow(DataRow("_sum" -> s))
        }
        else {
            val map = new mutable.HashMap[DataRow, Double]()
            this.rows.foreach(row => {
                val newRow = DataRow.from(row, groupBy: _*)
                if (map.contains(newRow)) {
                    map.update(newRow, map(newRow) + row.getDoubleOption(fieldName).getOrElse(0D))
                }
                else {
                    map.put(newRow, row.getDoubleOption(fieldName).getOrElse(0D))
                }
            })
            for ((row, s) <- map) {
                row.set("_sum", s)
                table.addRow(row)
            }
            map.clear()
        }
        
        table
    }
    
    //avg
    def avg(fieldName: String, groupBy: String*): DataTable = {
    
        case class AVG(private val v: Double = 0D) {
            
            var count = 0D
            var sum = 0D
            if (v > 0) this.plus(v)
            
            def plus(v: Double): Unit = {
                this.count += 1
                this.sum += v
            }
            
            def get(): Double = {
                if (count == 0) {
                    0D
                }
                else {
                    sum / count
                }
            }
        }
        
        val table = DataTable()
        if (groupBy.isEmpty) {
            var s = 0D
            this.rows.foreach(row => s += row.getDoubleOption(fieldName).getOrElse(0D))
            table.addRow(DataRow("_avg" -> s / this.count))
        }
        else {
            val map = new mutable.HashMap[DataRow, AVG]()
            this.rows.foreach(row => {
                val newRow = DataRow.from(row, groupBy: _*)
                if (map.contains(newRow)) {
                    map(newRow).plus(row.getDoubleOption(fieldName).getOrElse(0D))
                }
                else {
                    map.put(newRow, AVG(row.getDoubleOption(fieldName).getOrElse(0D)))
                }
            })
            for ((row, v) <- map) {
                row.set("_avg", v.get())
                table.addRow(row)
            }
            map.clear()
        }
    
        table
    }
    
    //max
    def max(fieldName: String, groupBy: String*): DataTable = {
    
        case class MAX(number: Option[Double] = None) {
            
            var max: Option[Double] = None
            if (number.nonEmpty) this.compare(number)
            
            def compare(value: Option[Double]): Unit = {
                value match {
                    case Some(v) =>
                        max match {
                            case Some(a) => Some(v max a)
                            case None => max = Some(v)
                        }
                    case None =>
                }
            }
            def get(): Option[Double] = max
        }
        
        val table = DataTable()
        if (groupBy.isEmpty) {
            var m = MAX()
            this.rows.foreach(row => {
                m.compare(row.getDoubleOption(fieldName))
            })
            table.addRow(DataRow("_max" -> m.get().getOrElse("none")))
        }
        else {
            val map = new mutable.HashMap[DataRow, MAX]()
            this.rows.foreach(row => {
                val newRow = DataRow.from(row, groupBy: _*)
                if (map.contains(newRow)) {
                    map(newRow).compare(row.getDoubleOption(fieldName))
                }
                else {
                    map.put(newRow, MAX(row.getDoubleOption(fieldName)))
                }
            })
            for ((row, m) <- map) {
                row.set("_max", m.get().getOrElse("none"))
                table.addRow(row)
            }
            map.clear()
        }
    
        table
    }
    
    //min
    def min(fieldName: String, groupBy: String*): DataTable = {
        
        case class MIN(number: Option[Double] = None) {
        
            var min: Option[Double] = None
            if (number.nonEmpty) this.compare(number)
        
            def compare(value: Option[Double]): Unit = {
                value match {
                    case Some(v) =>
                        min match {
                            case Some(a) => Some(v min a)
                            case None => min = Some(v)
                        }
                    case None =>
                }
            }
            def get(): Option[Double] = min
        }
    
        val table = DataTable()
        if (groupBy.isEmpty) {
            val m = MIN()
            this.rows.foreach(row => {
                m.compare(row.getDoubleOption(fieldName))
            })
            table.addRow(DataRow("_min" -> m.get().getOrElse("none")))
        }
        else {
            val map = new mutable.HashMap[DataRow, MIN]()
            this.rows.foreach(row => {
                val newRow = DataRow.from(row, groupBy: _*)
                if (map.contains(newRow)) {
                    map(newRow).compare(row.getDoubleOption(fieldName))
                }
                else {
                    map.put(newRow, MIN(row.getDoubleOption(fieldName)))
                }
            })
            for ((row, m) <- map) {
                row.set("_min", m.get().getOrElse("none"))
                table.addRow(row)
            }
            map.clear()
        }
    
        table
    }
    
    //take
    def take(amount: Int): DataTable = {
        val table = DataTable()
        for (i <- 0 until amount) {
            table.addRow(this.rows(i))
        }
        
        table
    }
    
    def insertRow(fields: (String, Any)*): DataTable = {
        this.addRow(DataRow(fields: _*))
        this
    }
    
    def updateWhile(filter: DataRow => Boolean)(setValue: DataRow => Unit): DataTable = {
        this.rows.foreach(row => {
            if (filter(row)) {
                setValue(row)
            }
        })
        this
    }
    
    def upsertRow(filter: DataRow => Boolean)(setValue: DataRow => Unit)(fields: (String, Any)*): DataTable = {
        var exists = false
        breakable {
            for(row <- this.rows) {
                if (filter(row)) {
                    setValue(row)
                    exists = true
                    break
                }
            }
        }
        if (!exists) {
            this.insertRow(fields: _*)
        }
        
        this
    }
    
    def deleteWhile(filter: DataRow => Boolean): DataTable = {
        val table = DataTable()
        this.rows.foreach(row => {
            if (!filter(row)) {
                table.addRow(row)
            }
        })
        this.clear()
        table
    }
    
    def selectRow(filter: DataRow => Boolean)(fieldNames: String*): DataTable = {
        val table = DataTable()
        this.rows.foreach(row => {
            if (filter(row)) {
                val newRow = DataRow()
                fieldNames.foreach(fieldName => {
                    newRow.set(fieldName, row.get(fieldName).orNull)
                })
                table.addRow(newRow)
            }
        })
        table
    }
    
    def updateSource(SQL: String): DataTable = {
        this.updateSource(DataSource.DEFAULT, SQL)
        this
    }
    
    def updateSource(dataSource: String, SQL: String): DataTable = {
        val ds = new DataSource(dataSource)
        ds.tableUpdate(SQL, this)
        ds.close()
        
        this
    }
    
    def nonEmpty: Boolean = {
        this.rows.nonEmpty
    }
    
    def isEmpty: Boolean = {
        this.rows.isEmpty
    }
    
    def copy(otherTable: DataTable): DataTable = {
        this.clear()
        this.union(otherTable)
        this
    }
    
    def cut(otherTable: DataTable): DataTable = {
        this.clear()
        this.merge(otherTable)
        this
    }
    
    def merge(otherTable: DataTable): DataTable = {
        this.union(otherTable)
        otherTable.clear()
        this
    }
    
    def union(otherTable: DataTable): DataTable = {
        this.fields ++= otherTable.fields
        this.labels ++= otherTable.labels
        this.rows ++= otherTable.rows
        this
    }
    
    def getFieldNames: List[String] = this.fields.keySet.toList
    def getLabelNames: List[String] = this.labels.values.toList
    def getLabels: mutable.LinkedHashMap[String, String] = this.labels
    def getFields: mutable.LinkedHashMap[String, DataType] = this.fields
    def getRows: mutable.ArrayBuffer[DataRow] = this.rows
    def columns: Int = this.fields.size
    def first: Option[DataRow] = if (this.rows.nonEmpty) Some(this.rows(0)) else None
    def last: Option[DataRow] = if (this.rows.nonEmpty) Some(this.rows(this.rows.size - 1)) else None
    
    def mkString(delimiter: String, fieldName: String): String = {
        val value = new StringBuilder()
        for (row <- this.rows) {
            if (value.nonEmpty) {
                value.append(delimiter)
            }
            value.append(row.getString(fieldName, "null"))
        }
        value.toString()
    }
    
    def mkString(prefix: String, delimiter: String, suffix: String): String = {
        val value = new StringBuilder()
        for (row <- this.rows) {
            value.append(prefix + row.join(delimiter) + suffix)
        }
        value.toString()
    }
    
    def show(limit: Int = 20): Unit = {
        writeLine("------------------------------------------------------------------------")
        writeLine(rows.size, " ROWS")
        writeLine("------------------------------------------------------------------------")
        writeLine(this.getFieldNames.mkString(", "))
        breakable {
            var i = 0
            for (row <- rows) {
                writeLine(row.join(", "))
                i += 1
                if (i >= limit) {
                    break
                }
            }
        }
        writeLine("------------------------------------------------------------------------")
    }
    
    override def toString: String = {
        val sb = new StringBuilder()
        for ((fieldName, dataType) <- this.fields) {
            if (sb.nonEmpty) {
                sb.append(",")
            }
            sb.append("\"" + fieldName + "\":\"" + dataType + "\"")
        }
        "{\"fields\":{" + sb.toString +"}, \"rows\":" + this.rows.asJava.toString + "}"
    }
    
    def toJsonString: String = {
        toString
    }
    
    def toHtmlString: String = {
        val sb = new StringBuilder()
        sb.append("""<table width="100%" cellpadding="4" cellspacing="0" border="0">""")
        sb.append("<tr>")
        this.fields.keySet.foreach(field => {
            sb.append("<th>")
            sb.append(this.labels(field))
            sb.append("</th>")
        })
        sb.append("</tr>")
        this.rows.foreach(row => {
            sb.append("<tr>")
            row.getValues.foreach(value => {
                sb.append("<td>")
                sb.append(value)
                sb.append("</td>")
            })
            sb.append("</tr>")
        })
        sb.append("</table>")
        
        sb.toString()
    }
    
    def clear(): Unit = {
        this.rows.clear()
        this.fields.clear()
        this.labels.clear()
    }
}
