package io.qross.model

import io.qross.util.{DataRow, DateTime}

//case class Message(value: Any)

case class Tick(minute: String)

object TaskStatus {
    val RESTARTING = "restarting"
    val INITIALIZED = "initialized"
    val READY = "ready"
    val EXECUTING = "executing"
}

case class Task(id: Long, var status: String = TaskStatus.INITIALIZED) {
    
    def INITIALIZED: Task = {
        this.status = TaskStatus.INITIALIZED
        this
    }
    
    def READY: Task = {
        this.status = TaskStatus.READY
        this
    }
    
    def EXECUTING: Task = {
        this.status = TaskStatus.EXECUTING
        this
    }
    
    def RESTARTING: Task = {
        this.status = TaskStatus.RESTARTING
        this
    }
}

case class TaskCommand(row: DataRow)