package io.qross.model

import io.qross.jdbc.DataSource

object Route {

    def getRunningActionsOfJob(jobId: Int): List[Long] = {
        DataSource.QROSS.querySingleList[Long]("SELECT id FROM qross_tasks_dags WHERE job_id=? AND status='running'", jobId)
    }

    def getRunningActionsOfTask(taskId: Long): List[Long] = {
        DataSource.QROSS.querySingleList[Long]("SELECT id FROM qross_tasks_dags WHERE task_id=? AND status='running'", taskId)
    }

    def getRunningAction(actionId: Long): Option[Long] = {
        DataSource.QROSS.querySingleValue("SELECT id FROM qross_tasks_dags WHERE id=? AND status='running'", actionId).toOption[Long]
    }

    def isNoteQuerying(noteId: Long): Boolean = {
        QrossNote.QUERYING.contains(noteId)
    }
}
