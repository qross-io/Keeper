package io.qross.util

import java.io._

import io.qross.model.Global

import scala.collection.immutable.HashMap
import scala.util.{Success, Try}

object Properties {
    
    private val props = new java.util.Properties()
    private val externalPath = new File(Properties.getClass.getProtectionDomain.getCodeSource.getLocation.getPath).getParentFile.getAbsolutePath.replace("\\", "/") + "/qross.ds.properties"
    //private val internalPath = Properties.getClass.getResource("/conf.properties").toString
    //private lazy val externalOutput = new FileOutputStream(internalPath)
    
    props.load(new BufferedReader(new InputStreamReader(Properties.getClass.getResourceAsStream("/conf.properties"))))
    load(externalPath)
    
    def loadAll(files: String*): Unit = {
        if (!load(externalPath)
             && files.isEmpty) {
            Output.writeExceptions("Please assign at lest one properties file which contains database connections.")
            System.exit(1)
        }
        else {
            files.foreach(path => {
                load(path)
            })
        }
        
        if (!props.containsKey(DataSource.DEFAULT)) {
            Output.writeExceptions(s"Can't find properties key ${DataSource.DEFAULT}, it must be set in conf.properties, qross.ds.properties or other properties files you loaded.")
            System.exit(1)
        }
        else if (!DataSource.testConnection()) {
            Output.writeExceptions(s"Can't open database, please check your connection string of ${DataSource.DEFAULT}.")
            System.exit(1)
        }
        else {
            var version = ""
            try {
                version = Global.QROSS_VERSION
            }
            catch {
                case e: Exception =>
            }
            
            if (version != "") {
                Output.writeMessage("Welcome to QROSS Keeper v" + version)
            }
            else {
                Output.writeExceptions("Can't find Qross system, please create your qross system use Qross Master.")
                System.exit(1)
            }
        }
    }
    
    def load(path: String): Boolean = {
        val file = new File(path)
        if (file.exists()) {
            props.load(new BufferedInputStream(new FileInputStream(file)))
        }
        file.exists()
    }
    
    def get(key: String, defaultValue: String = ""): String = {
        if (props.containsKey(key)) {
            props.getProperty(key)
        }
        else {
            defaultValue
        }
    }
    
    def getInt(key: String, defaultValue: Int = 0): Int = {
        if (props.containsKey(key)) {
            Try(props.getProperty(key).toInt) match {
                case Success(value) => value
                case _ => defaultValue
            }
        }
        else {
            defaultValue
        }
    }
    
    def getBoolean(key: String): Boolean = {
        if (props.containsKey(key)) {
            props.getProperty(key).toLowerCase() match {
                case "true" | "yes" | "ok" | "1" => true
                case _ => false
            }
        }
        else {
            false
        }
    }
    
    /*
    def set(key: String, value: String): Unit = {
        props.setProperty(key, value)
        props.store(externalOutput, "updated by user: " + key + " = " + value)
    }
    
    def getDataSources: HashMap[String, String] = {
        var sources = new HashMap[String, String]
        props.entrySet().forEach(row => {
            sources += (row.getKey.toString -> row.getValue.toString)
        })
        
        sources
    }*/
    
    def getDataExchangeDirectory: String = {
        var directory = Properties.get("data_exchange_directory")
        directory = directory.replace("\\", "/")
        if (!directory.endsWith("/")) {
            directory += "/"
        }
        directory
    }
}