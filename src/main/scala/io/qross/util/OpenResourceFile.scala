package io.qross.util

import scala.io.Source

case class OpenResourceFile(path: String) {

    //private val source = Source.fromFile(this.getClass.getResource(path).getPath, "UTF-8")
    private val source = Source.fromInputStream(this.getClass.getResourceAsStream(path), "UTF-8")
    private var content: String = source.mkString
    source.close()

    def replace(oldStr: String, newStr: String): OpenResourceFile = {
        content = content.replace(oldStr, newStr)
        this
    }
    
    def replaceWith(row: DataRow): OpenResourceFile = {
        row.foreach((key, value) => {
            content = content.replace("${" + key + "}", if (value != null) value.toString else "")
        })
        this
    }
    
    def writeEmail(title: String): Email = {
        Email.write(title).setContent(content)
    }
    
    override def toString: String = {
        content
    }
}