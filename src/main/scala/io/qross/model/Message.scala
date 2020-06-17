package io.qross.model

import io.qross.core.DataRow

//case class Message(value: Any)

case class Tick(minute: String)

object JobType {
    val SCHEDULED = "scheduled"
    val ENDLESS = "endless"
    val DEPENDENT = "dependent"
    val MANUAL = "manual"
}

object TaskStatus {
    val NEW = "new"
    val INSTANT = "instant"
    val NO_COMMANDS = "no_commands"
    //val MANUAL = "manual"
    //val RESTARTING = "restarting"
    val INITIALIZED = "initialized"
    val READY = "ready"
    val EXECUTING = "executing"
    val CHECKING_LIMIT = "checking_limit"
    val FINISHED = "finished"
    val FAILED = "failed"
    val INCORRECT = "incorrect"
    val TIMEOUT = "timeout"
    val SUCCESS = "success"
    val IGNORE = "ignore"
    val EMPTY = ""
    val INTERRUPTED = "interrupted"
    val SLOW = "slow"
}

object ActionStatus {
    val WAITING = "waiting"
    val QUEUEING = "queueing"
    val WRONG = "wrong"
    val RUNNING = "running"
    val EXCEPTIONAL = "exceptional"
    val OVERTIME = "overtime"
    val DONE = "done"
    val KILLED = "killed"
}

case class Task(id: Long, var status: String = TaskStatus.INITIALIZED) {

    var jobId: Int = 0
    var taskTime: String = ""
    var recordTime: String = ""

    def of(jobId: Int): Task = {
        this.jobId = jobId
        this
    }

    def at(taskTime: String, recordTime: String): Task = {
        this.taskTime = taskTime
        this.recordTime = recordTime
        this
    }

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
}

case class TaskCommand(row: DataRow)

case class Note(noteId: Long, userId: Int)