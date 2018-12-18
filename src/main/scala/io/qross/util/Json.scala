package io.qross.util

import java.io.{File, InputStream}
import java.net.{HttpURLConnection, MalformedURLException, URL}

import com.fasterxml.jackson.databind.node.JsonNodeType
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import io.qross.model.Global
import org.json4s.{Formats, NoTypeHints}
import org.json4s.jackson.Serialization

import scala.collection.mutable
import scala.util.{Failure, Success, Try}


object Json {
   
    //add toJson method for List
    implicit class ListExt[T](list: List[T]) {
        def toJson: String = {
            Json.toString(list)
        }
    }
    
    def fromText(text: String): Json = Json()
    def fromURL(url: String, post: String = ""): Json = Json().readURL(url, post)

    def toString(obj: AnyRef): String = {
        implicit val formats: Formats = Serialization.formats(NoTypeHints)
        Serialization.write(obj)
    }
}

case class Json(text: String = "") {
    
    private val mapper = new ObjectMapper
    private var root: JsonNode = _
    
    if (text != "") {
        root = mapper.readTree(text)
    }
    
    def readURL(url: String, method: String = ""): Json = {
        try {
            val URL = new URL(if (url.contains("://")) url else "http://" + url)
            if (method == "") {
                root = mapper.readTree(URL)
            }
            else {
                val conn = URL.openConnection().asInstanceOf[HttpURLConnection]
                conn.setDoOutput(true)
                conn.setDoInput(true)
                //conn.addRequestProperty("Content-Type", "application/json; charset=utf-8");
                conn.setRequestMethod(method.toUpperCase())
                conn.connect()
    
                val os = conn.getOutputStream
                os.write(method.getBytes(Global.CHARSET))
                os.close()
    
                val is = conn.getInputStream
                root = mapper.readTree(is)
                is.close()
            }
        }
        catch {
            case e: MalformedURLException => e.printStackTrace()
            case o: Exception => o.printStackTrace()
        }
        this
    }
    
    def readStream(inputStream: InputStream): Json = {
        root = mapper.readTree(inputStream)
        this
    }

    def findDataTable(path: String): DataTable = {
        val table = new DataTable
        
        val node = findNode(path)
        if (node.isArray) {
            node.elements().forEachRemaining(child => {
                val row = DataRow()
                if (child.isObject) {
                    child.fields().forEachRemaining(item => {
                        //table.addField(item.getKey, DataType.from(node))
                        row.set(item.getKey, getValue(item.getValue))
                    })
                }
                else if (child.isArray)  {
                    child.elements().forEachRemaining(item => {
                        //table.addField("c" + row.size, DataType.from(item))
                        row.set("c" + row.size, getValue(item))
                    })
                }
                else {
                    //table.addField("value", DataType.from(child))
                    row.set("value", getValue(child))
                }
                table.addRow(row)
            })
        }
        else if (node.isObject) {
            val row = DataRow()
            node.fields().forEachRemaining(child => {
                //table.addField(child.getKey, DataType.from(child.getValue))
                row.set(child.getKey, getValue(child.getValue))
            })
            table.addRow(row)
        }
        else {
            val row = DataRow()
            //table.addField("value", DataType.from(node))
            row.set("value", getValue(node))
            table.addRow(row)
        }
        
        table
    }


    def findDataRow(path: String): DataRow = {
        
        val row = new DataRow
        
        val node = findNode(path)
        if (node.isObject) {
            node.fields().forEachRemaining(child => {
                row.set(child.getKey, getValue(child.getValue))
            })
        }
        else if (node.isArray) {
            node.elements().forEachRemaining(child => {
                row.set("c" + row.size, getValue(child))
            })
        }
        else {
            row.set("value", getValue(node))
        }
        
        row
    }
    
    def findList(path: String): List[Any] = {
        val list = new mutable.ListBuffer[Any]()
        
        val node = findNode(path)
        if (node.isArray) {
            node.elements().forEachRemaining(child => {
                list += getValue(child)
            })
        }
        else {
            list += node.toString
        }
        
        list.toList
    }
    
    def findValue(path: String): Any = {
        getValue(findNode(path))
    }
    
    private def getValue(node: JsonNode): Any = {
        node.getNodeType match {
            case JsonNodeType.ARRAY => node.toString
            case JsonNodeType.BOOLEAN => if (node.booleanValue()) 1 else 0
            case JsonNodeType.BINARY => node.toString
            case JsonNodeType.MISSING => "MISSING"
            case JsonNodeType.NULL => "NULL"
            case JsonNodeType.NUMBER => {
                    if (node.isIntegralNumber) {
                        if (node.isInt) {
                            node.intValue()
                        }
                        else {
                            node.longValue()
                        }
                    }
                    else {
                        if (node.isFloat) {
                            node.floatValue()
                        }
                        else {
                            node.doubleValue()
                        }
                    }
                }
            case JsonNodeType.OBJECT => node.toString
            case JsonNodeType.POJO => node.toString
            case JsonNodeType.STRING => node.textValue()
        }
    }
    
    /* Can work at cluster mode. e.g. hadoop jar keeper.jar
    def findNode(path: String): JsonNode = {
        root.at(if (path.endsWith("/")) path.dropRight(1) else path)
    } */
    
    private def findNode(path: String): JsonNode = {
        var p = path
        if (p.startsWith("/")) p.drop(1)
        
        var node: JsonNode = root
        while (!node.isNull && !p.isEmpty) {
            
            val section = if (p.contains("/")) p.substring(0, p.indexOf("/")).trim else p.trim
            p = if (p.contains("/")) p.substring(p.indexOf("/") + 1) else ""
            
            if (!section.isEmpty) {
                if (node.isArray) {
                    node = Try(section.toInt) match {
                        case Success(v) => node.get(v)
                        case Failure(_) => node.get(0)
                    }
                }
                else if (node.isObject) {
                    node = node.get(section)
                }
            }
        }
        
        node
    }
}
