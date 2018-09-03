package io.qross.util

import io.qross.model.TaskEvent
import io.qross.util.DataType.DataType
import io.qross.util.Output._

import scala.collection.mutable

class DataHub (defaultSourceName: String = DataSource.DEFAULT) {
    
    private val SOURCES = mutable.HashMap[String, DataSource](
        "CACHE" -> DataSource.createMemoryDatabase,
        "DEFAULT" -> DataSource.create(defaultSourceName)
    )
    
    private var DEBUG = false
    
    private val CACHE = SOURCES("CACHE") //SQLite cache
    private val DEFAULT = SOURCES("DEFAULT") //default dataSource
    private var CURRENT = DEFAULT   //current dataSource - open
    private var TARGET = DEFAULT    //current dataDestination - saveAs
    
    private val TABLE = DataTable()  //current buffer
    private val BUFFER = new mutable.HashMap[String, DataTable]() //all buffer
    
    private var JSON: Json = _
    private var TO_BE_CLEAR: Boolean = false
    
    // ---------- system ----------
    
    def debug(): DataHub = {
        DEBUG = true
        this
    }
    
    // ---------- open ----------
    
    def openCache(): DataHub = {
        CURRENT = CACHE
        this
    }
    
    def openDefault(): DataHub = {
        CURRENT = DEFAULT
        this
    }
    
    def openTarget(): DataHub = {
        CURRENT = TARGET
        this
    }
    
    def open(connectionName: String): DataHub = {
        if (!SOURCES.contains(connectionName)) {
            SOURCES += connectionName -> new DataSource(connectionName)
        }
        CURRENT = SOURCES(connectionName)
        //CONNECTING = connectionName
        this
    }
    
    // ---------- save as ----------
    
    def saveAsCache(): DataHub = {
        TARGET = CACHE
        this
    }
    
    def saveAsDefault(): DataHub = {
        TARGET = DEFAULT
        this
    }
    
    def saveAs(connectionName: String): DataHub = {
        if (!SOURCES.contains(connectionName)) {
            SOURCES += connectionName -> new DataSource(connectionName)
        }
        TARGET = SOURCES(connectionName)
        this
    }
    
    def saveAsTextFile(fileNameOrFullPath: String, delimiter: String = ",", deleteFileIfExists: Boolean = true): DataHub = {
        FileWriter(fileNameOrFullPath, deleteFileIfExists).delimit(delimiter).writeTable(TABLE).close()
        this
    }
    
    def saveAsCsvFile(fileNameOrFullPath: String, deleteFileIfExists: Boolean = true): DataHub = {
        FileWriter(fileNameOrFullPath, deleteFileIfExists).delimit(",").writeTable(TABLE).close()
        this
    }
    
//    def saveAsExcel(fileNameOrFullPath: String, sheetName: String = "sheet1"): DataHub = {
//        Excel(fileNameOrFullPath).writeTable(TABLE, sheetName)
//        this
//    }
    
    // ---------- cache ----------
    
    def cache(tableName: String): DataHub = {
        this.cache(tableName, TABLE)
        this
    }
    
    def cache(tableName: String, table: DataTable): DataHub = {
        //var createSQL = "CREATE TABLE IF NOT EXISTS " + tableName + " (__pid INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL UNIQUE"
        var createSQL = ""
        val placeHolders = new mutable.ArrayBuffer[String]
        for ((field, dataType) <- table.getFields) {
            if (createSQL.nonEmpty) {
                createSQL += ", "
            }
            createSQL += field + " " + dataType.toString
            placeHolders += "?"
        }
        createSQL = "CREATE TABLE IF NOT EXISTS " + tableName + " (" + createSQL + ");"
    
        if (DEBUG) {
            table.show(10)
            writeMessage(createSQL)
        }
        
        CACHE.executeNonQuery(createSQL)
        
        if (table.nonEmpty) {
            CACHE.setBatchCommand("INSERT INTO " + tableName + " (" + table.getFieldNames.mkString(",") + ") VALUES (" + placeHolders.mkString(",") + ");")
            table.foreach(row => {
                CACHE.addBatch(row.getValues)
            })
            CACHE.executeBatchUpdate()
        }
        placeHolders.clear()
        
        TO_BE_CLEAR = true
        
        this
    }
    
    // ---------- base method ----------

    def get(selectSQL: String, values: Any*): DataHub = {
        if (TO_BE_CLEAR) {
            TABLE.clear()
            TO_BE_CLEAR = false
        }
        
        TABLE.merge(CURRENT.executeDataTable(selectSQL, values: _*))
        this
    }

    //execute SQL on source dataSource
    def set(nonQuerySQL: String, values: Any*): DataHub = {
        CURRENT.executeNonQuery(nonQuerySQL, values: _*)
        this
    }

    def join(selectSQL: String, on: (String, String)*): DataHub = {
        TABLE.join(CURRENT.executeDataTable(selectSQL), on: _*)
        this
    }
    
    //execute SQL on target dataSource
    def fit(nonQuerySQL: String, values: Any*): DataHub = {
        TARGET.executeNonQuery(nonQuerySQL, values: _*)
        this
    }
   
    def put(nonQuerySentence: String): DataHub = {
        if (DEBUG) TABLE.show(10)
        
        TARGET.tableUpdate(nonQuerySentence, TABLE)
        TO_BE_CLEAR = true
        this
    }
    
    def put(nonQuerySentence: String, table: DataTable): DataHub = {
        TARGET.tableUpdate(nonQuerySentence, table)
        this
    }
    
    // ---------- buffer basic ----------
    
    //switch table
    def from(tableName: String): DataHub = {
        TABLE.clear()
        
        if (BUFFER.contains(tableName)) {
            TABLE.union(BUFFER(tableName))
        }
        else {
            throw new Exception(s"There is no table named $tableName in buffer.")
        }
        this
    }
    
    def buffer(tableName: String, table: DataTable): DataHub = {
        BUFFER += tableName -> table
        TABLE.copy(table)
        this
    }
    
    def buffer(table: DataTable): DataHub = {
        TABLE.copy(table)
        this
    }
    
    def buffer(tableName: String): DataHub = {
        BUFFER += tableName -> DataTable.from(TABLE)
        this
    }
    
    def merge(table: DataTable): DataHub = {
        TABLE.merge(table)
        this
    }
    
    def merge(tableName: String, table: DataTable): DataHub = {
        if (BUFFER.contains(tableName)) {
            BUFFER(tableName).merge(table)
        }
        else {
            BUFFER += tableName -> table
        }
        this
    }
    
    def union(table: DataTable): DataHub = {
        TABLE.union(table)
        this
    }
    
    def union(tableName: String, table: DataTable): DataHub = {
        if (BUFFER.contains(tableName)) {
            BUFFER(tableName).union(table)
        }
        else {
            BUFFER += tableName -> table
        }
        this
    }
    
    def takeOut(): DataTable = {
        TABLE
    }
    
    def takeOut(tableName: String): DataTable = {
        if (BUFFER.contains(tableName)) {
            BUFFER(tableName)
        }
        else {
            DataTable()
        }
    }

   
    def discard(tableName: String): DataHub = {
        if (BUFFER.contains(tableName)) {
            BUFFER.remove(tableName)
        }
        this
    }
    
    def nonEmpty: Boolean = {
        TABLE.nonEmpty
    }
    
    def isEmpty: Boolean = {
        TABLE.isEmpty
    }
    
    // ---------- buffer action ----------
    
    def label(alias: (String, String)*): DataHub = {
        TABLE.label(alias: _*)
        this
    }
    
    def pass(querySentence: String, default:(String, Any)*): DataHub = {
        if (TABLE.isEmpty) {
            if (default.nonEmpty) {
                TABLE.addRow(DataRow(default: _*))
            }
            else {
                throw new Exception("No data to pass. Please ensure data exists or default value provided.")
            }
        }
        TABLE.cut(CURRENT.tableSelect(querySentence, TABLE))
        
        this
    }
    
    def foreach(callback: (DataRow) => Unit): DataHub = {
        TABLE.foreach(callback)
        this
    }
    
    def map(callback: (DataRow) => DataRow) : DataHub = {
        TABLE.cut(TABLE.map(callback))
        this
    }
    
    def table(fields: (String, DataType)*)(callback: (DataRow) => DataTable): DataHub = {
        TABLE.cut(TABLE.table(fields: _*)(callback))
        this
    }
    
    def flat(callback: (DataTable) => DataRow): DataHub = {
        val row = callback(TABLE)
        TABLE.clear()
        TABLE.addRow(row)
        this
    }
    
    def filter(callback: (DataRow) => Boolean): DataHub = {
        TABLE.cut(TABLE.filter(callback))
        this
    }
    
    def collect(filter: DataRow => Boolean)(map: DataRow => DataRow): DataHub = {
        TABLE.cut(TABLE.collect(filter)(map))
        this
    }
    
    def distinct(fieldNames: String*): DataHub = {
        TABLE.cut(TABLE.distinct(fieldNames: _*))
        this
    }
    
    def count(groupBy: String*): DataHub = {
        TABLE.cut(TABLE.count(groupBy: _*))
        this
    }
    
    def sum(fieldName: String, groupBy: String*): DataHub = {
        TABLE.cut(TABLE.sum(fieldName, groupBy: _*))
        this
    }
    
    def avg(fieldName: String, groupBy: String*): DataHub = {
        TABLE.cut(TABLE.avg(fieldName, groupBy: _*))
        this
    }
    
    def min(fieldName: String, groupBy: String*): DataHub = {
        TABLE.cut(TABLE.min(fieldName, groupBy: _*))
        this
    }
    
    def max(fieldName: String, groupBy: String*): DataHub = {
        TABLE.cut(TABLE.max(fieldName, groupBy: _*))
        this
    }
    
    def take(amount: Int): DataHub = {
        TABLE.cut(TABLE.take(amount))
        this
    }

    def insertRow(fields: (String, Any)*): DataHub = {
        TABLE.insertRow(fields: _*)
        this
    }
    
    def insertRowIfEmpty(fields: (String, Any)*): DataHub = {
        if (TABLE.isEmpty) {
            TABLE.insertRow(fields: _*)
        }
        this
    }
    
    //def updateRow
    
    // ---------- Json & Api ---------
    
    def openJson(): DataHub = {
        this
    }
    
    def openJson(jsonText: String): DataHub = {
        JSON = Json.fromText(jsonText)
        this
    }
    
    def openJsonApi(url: String): DataHub = {
        JSON = Json.fromURL(url)
        this
    }
    
    def openJsonApi(url: String, post: String): DataHub = {
        JSON = Json.fromURL(url, post)
        this
    }
    
    def find(jsonPath: String): DataHub = {
        TABLE.copy(JSON.findDataTable(jsonPath))
        this
    }
    
    
    // ---------- dataSource ----------
  
    def executeDataTable(SQL: String, values: Any*): DataTable = CURRENT.executeDataTable(SQL, values: _*)
    def executeDataRow(SQL: String, values: Any*): DataRow = CURRENT.executeDataRow(SQL, values: _*)
    def executeSingleValue(SQL: String, values: Any*): Option[String] = CURRENT.executeSingleValue(SQL, values: _*)
    def executeExists(SQL: String, values: Any*): Boolean = CURRENT.executeExists(SQL, values: _*)
    def executeNonQuery(SQL: String, values: Any*): Int = CURRENT.executeNonQuery(SQL, values: _*)
    
    // ---------- Json Basic ----------
    
    def findDataTable(jsonPath: String): DataTable = JSON.findDataTable(jsonPath)
    def findDataRow(jsonPath: String): DataRow = JSON.findDataRow(jsonPath)
    def findList(jsonPath: String): List[Any] = JSON.findList(jsonPath)
    def findValue(jsonPath: String): Any = JSON.findValue(jsonPath)

    // ---------- for Keeper ----------

    def writeKeeperEmail(taskStatus: String): DataHub = {
        TABLE.first match {
            case Some(row) =>  TaskEvent.sendMail(taskStatus, row, BUFFER("logs"))
            case None =>
        }
        this
    }

    def requestKeeperApi(taskStatus: String): DataHub = {
        TABLE.first match {
            case Some(row) =>  TaskEvent.requestApi(taskStatus, row)
            case None =>
        }
        this
    }
    
    // ---------- other ----------
    
    def close(): Unit = {
        SOURCES.values.foreach(_.close())
        BUFFER.clear()
        TABLE.clear()
    }
}