package io.qross.util


import java.io.BufferedReader
import java.io.FileInputStream
import java.io.IOException
import java.io.InputStream
import java.io.InputStreamReader
import java.util.Scanner
import java.util.zip.GZIPInputStream

import io.qross.model.Global
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

class HDFSReader(path: String) {
    
    private val conf = new Configuration()
    conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem")
    
    val hdfs: FileSystem  = FileSystem.get(conf)
    val file = new Path(path)
    
    val extension: String = this.path.substring(path.lastIndexOf("."))
    
    def isFile: Boolean = hdfs.isFile(file)
    if (!isFile) throw new IOException(s"Incorrect file input: $path")
    
    val inputStream: InputStream = if (isFile) hdfs.open(file) else null
    
    val scanner: Scanner =
        if (".log".equalsIgnoreCase(extension) || ".txt".equalsIgnoreCase(extension)) {
            new Scanner(inputStream, Global.CHARSET)
        } else if (".gz".equalsIgnoreCase(extension)) {
            new Scanner(new GZIPInputStream(inputStream), Global.CHARSET)
        } else if (".bz2".equalsIgnoreCase(extension)) {
            new Scanner(new BZip2CompressorInputStream(inputStream), Global.CHARSET)
        } else {
            throw new IOException(s"Unrecognized format: $path")
        }

    def hasNextLine: Boolean = {
        this.scanner.hasNextLine
    }
    
    def readLine: String = {
        this.scanner.nextLine()
    }
    
    def close(): Unit = {
        inputStream.close()
        scanner.close()
        hdfs.close()
        conf.clear()
    }
}