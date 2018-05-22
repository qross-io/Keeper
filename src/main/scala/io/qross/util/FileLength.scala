package io.qross.util

import scala.util.{Success, Try}

object FileLength {
    
    def main(args: Array[String]): Unit = {
        //println(toHumanizedString(1234567890))
    }
    
    def toByteLength(length: String): Long = {
        var size = length
        var number = length.dropRight(1)
        var times: Long = length.takeRight(1).toUpperCase() match {
            case "B" => 1
            case "K" => 1024
            case "M" => 1024 * 1024
            case "G" => 1024 * 1024 * 1024
            case "T" => 1024 * 1024 * 1024 * 1024
            case "P" => 1024 * 1024 * 1024 * 1024 * 1024
            case b =>
                Try(b.toInt) match {
                    case Success(n) => number += n
                    case _ =>
                }
                1
        }
        
        Try(number.toLong) match {
            case Success(n) => n * times
            case _ => 0
        }
    }
    
    def toHumanizedString(length: Long): String = {
        var size: Double = length
        var units = List("B", "K", "M", "G", "T", "P")
        var i = 0
        while (size > 1024 && i < 6) {
            size /= 1024
            i += 1
        }
        
        f"$size%.2f${units(i)}"
    }
}
