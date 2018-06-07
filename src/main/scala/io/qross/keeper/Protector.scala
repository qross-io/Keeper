package io.qross.keeper

import io.qross.model.Global

import scala.sys.process._

object Protector {
    def main(args: Array[String]): Unit = {
    
        val command = "hadoop jar io.qross.keeper-0.5.jar io.qross.keeper.Keeper"
    
        var exitValue = 1
        val logger = new Logger()
        while(exitValue != 0) {
            exitValue = command.!(ProcessLogger(out => {
                //logger.log(out)
            }, err => {
                //logger.err(err)
            }))
            
            if (exitValue != 0) {
                //send mail
            }
        }
        //start/restart keeper
        //record error
        //send mail to keeper admin : errors, restart, beats
    }
}
