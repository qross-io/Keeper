package io.qross.keeper

import io.qross.jdbc.DataSource
import io.qross.model.{ActionStatus, TaskStatus, WorkActor}
import io.qross.setting.Environment

class Monitor extends WorkActor {
    override def beat(tick: String): Unit = {
        super.beat(tick)

        val ds = DataSource.QROSS

        val executingTasks = ds.executeSingleValue(s"SELECT COUNT(0) AS tasks FROM qross_tasks WHERE status='${TaskStatus.EXECUTING}'").asInteger
        val runningActions = ds.executeSingleValue(s"SELECT COUNT(0) AS actions FROM qross_tasks_dags WHERE status='${ActionStatus.RUNNING}'").asInteger
        val logsCount = ds.executeSingleValue(s"SELECT COUNT(0) AS logs FROM qross_tasks_logs").asInteger

        ds.executeNonQuery(s"INSERT INTO qross_server_monitor (moment, cpu_usage, memory_usage, jvm_memory_usage, executing_tasks, running_actions, logs_count) VALUES ('$tick', ${Environment.cpuUsage}, ${Environment.systemMemoryUsage}, ${Environment.jvmMemoryUsage}, $executingTasks, $runningActions, $logsCount)")

        ds.close()
    }
}