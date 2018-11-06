package io.qross.util

import java.io.IOException

import io.qross.model.Global.CONFIG
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.collection.mutable

object HDFS {
    
    def KERBEROS_AUTH: Boolean = CONFIG.getBoolean("KERBEROS_AUTH")
    def KRB_USER_PRINCIPAL: String = CONFIG.getString("KRB_USER_PRINCIPAL")
    def KRB_KEYTAB_PATH: String = CONFIG.getString("KRB_KEYTAB_PATH")
    def KRB_KRB5CONF_PATH: String = CONFIG.getString("KRB_KRB5CONF_PATH")

    private val conf = new Configuration()
    conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem")
    
    if (KERBEROS_AUTH) {
        KerberosLogin.login(KRB_USER_PRINCIPAL, KRB_KEYTAB_PATH, KRB_KRB5CONF_PATH, conf)
    }
    
    private lazy val fileSystem = FileSystem.get(conf)
    
    def list(path: String): List[HDFS] = {
        val dir = new Path(path)
        val list = new mutable.ListBuffer[HDFS]()
        
        try {
            val files = fileSystem.globStatus(dir)
            if (files.nonEmpty) {
                for (f <- files) {
                    var p = f.getPath.toString
                    p = p.substring(p.indexOf("//") + 2)
                    p = p.substring(p.indexOf("/"))
                
                    list += HDFS(p, f.getLen, f.getModificationTime)
                    //f.getLen - byte
                    //f.getModificationTime - ms
                }
            }
        } catch {
            case e: IOException => e.printStackTrace()
        }
    
        list.toList
    }
}

case class HDFS(path: String, size: Long, lastModificationTime: Long)