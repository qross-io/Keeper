package io.qross.model

import io.qross.util.Output._
import io.qross.util._

import scala.sys.process._
import scala.util.{Success, Try}

object QrossTask {
  
    //TaskProducer
    //on start up
    def complementTasks(): Unit = {
        
        /*
        TimeLine:
        last beat -> server offline -> now -> server online -> next beat
        */
        val dh = new DataHub()
    
        //get last tick of producer
        val lastBeat = (Try(dh.openDefault().executeSingleValue("SELECT UNIX_TIMESTAMP(last_beat_time) FROM qross_keeper_beats WHERE actor_name='TaskProducer'").getOrElse("").toLong) match {
                case Success(tick) => DateTime.of(tick)
                case _ => DateTime.now
            }).setSecond(0).setNano(0).plusMinutes(1)
        val nextBeat = DateTime.now.setSecond(0).setNano(0).plusMinutes(1)
        
        //get all jobs
        dh.openDefault()
            //next_tick != '' means this is not a new job
            .get(s"SELECT id AS job_id, cron_exp, next_tick, complement_missed_tasks FROM qross_jobs WHERE enabled='true' AND cron_exp!='' AND next_tick!='' AND next_tick!='NONE' AND next_tick<${nextBeat.getTickValue}")
                .cache("jobs")
    
        //get all ticks for jobs that need to complement during server was offline
        dh.openCache()
            .get("SELECT job_id, cron_exp, next_tick FROM jobs WHERE complement_missed_tasks='yes'")
                .table("job_id" -> DataType.INTEGER, "next_tick" -> DataType.TEXT) (row => {
                    val table = DataTable()
                    val jobId = row.getInt("job_id")
                    CronExp.getTicks(row.getString("cron_exp"), lastBeat.getTickValue, nextBeat.getTickValue).foreach(time => {
                        table.insertRow(
                            "job_id" -> jobId,
                            "next_tick" -> time
                        )
                    })
                    table
                }).cache("missed_tasks")
    
        //get exists tasks during offline
        dh.openDefault()
            .get(s"SELECT job_id, task_time FROM qross_tasks WHERE job_id IN (SELECT id FROM qross_jobs WHERE enabled='true' AND cron_exp!='' AND next_tick!='' AND next_tick!='NONE' AND complement_missed_tasks='yes') AND task_time>${lastBeat.getTickValue} AND task_time<${nextBeat.getTickValue}")
                .cache("exists_tasks")
        
        //complement all jobs
        dh.openCache()
            .get(s"SELECT A.job_id, A.next_tick FROM missed_tasks A LEFT JOIN exists_tasks B ON A.job_id=B.job_id AND A.next_tick=B.task_time WHERE B.job_id IS NULL")
                .put("INSERT INTO qross_tasks (job_id, task_time) VALUES (?, ?)")
    
        //get next tick for all jobs
        dh.openCache()
            .get("SELECT job_id, cron_exp, next_tick FROM jobs")
                .foreach(row => {
                    row.set("next_tick", CronExp(row.getString("cron_exp")).getNextTickOrNone(nextBeat))
                }).put("UPDATE qross_jobs SET next_tick=$next_tick WHERE id=$job_id")
        
        //restart executing tasks when Keeper exit exceptionally.
        dh.openDefault()
            .get("SELECT A.task_id FROM (SELECT id As task_id FROM qross_tasks WHERE status='executing') A INNER JOIN qross_jobs B ON A.job_id=B.id AND B.enabled='true'")
            .put("UPDATE qross_tasks_dags SET status='exceptional' WHERE task_id=#task_id AND status IN ('queuing', 'running')")
            .put("UPDATE qross_tasks SET status='restarting',start_time=NULL,finish_time=NULL,spent=NULL,update_time=NOW() WHERE id=#task_id;")
            .put("INSERT INTO qross_message_box (message_type, message_key, message_text) VALUES ('TASK', 'RESTART', '^EXCEPTIONAL@#task_id')")
        
        dh.close()
    }
    
    //TaskProducer
    //create and initialize tasks then return initialized and ready tasks
    def createAndInitializeTasks(tick: String): DataTable = {
        val minute = DateTime(tick)
        
        val dh = new DataHub()
        
        //update empty next_tick - it will be empty when create a new job
        //update outdated jobs - it will occur when you enable a job from disabled status
        dh.openDefault()
            .get(s"SELECT id AS job_id, cron_exp, '' AS next_tick FROM qross_jobs WHERE  cron_exp!='' AND (next_tick='' OR (next_tick!='NONE' AND next_tick<$tick))")
                .foreach(row => row.set("next_tick", CronExp(row.getString("cron_exp")).getNextTickOrNone(minute)))
                    .put("UPDATE qross_jobs SET next_tick=$next_tick WHERE id=$job_id")
        
        //create tasks without cron_exp
        //excluding jobs with running tasks
        dh.openDefault()
            .executeNonQuery("INSERT INTO qross_tasks (job_id, task_time) SELECT id, '' FROM qross_jobs WHERE cron_exp='' AND enabled='true' AND id NOT IN (SELECT job_id FROM qross_tasks WHERE task_time='' AND status NOT IN ('finished', 'incorrect', 'failed'))")
        
        //jobs with cron_exp
        dh.openDefault()
            .get(s"SELECT id AS job_id, cron_exp, next_tick FROM qross_jobs WHERE next_tick='$tick' AND enabled='true' AND id NOT IN (SELECT job_id FROM qross_tasks WHERE task_time='$tick')")
                .cache("jobs")
            .get(s"SELECT id AS task_id, job_id, task_time FROM qross_tasks WHERE task_time='$tick'")
                .cache("exists_tasks")
        
        //create schedule tasks
        dh.openCache()
            .get("SELECT A.job_id, A.next_tick FROM jobs A LEFT JOIN exists_tasks B ON A.job_id=B.job_id AND A.next_tick=B.task_time WHERE B.job_id IS NULL")
                .put("INSERT INTO qross_tasks (job_id, task_time) VALUES (?, ?)")
        
        //get next tick and update jobs
        minute.plusMinutes(1) //get next minute to match next tick
        dh.openCache()
            .get("SELECT job_id, cron_exp, '' AS next_tick FROM jobs")
                .foreach(row => {
                    row.set("next_tick", CronExp(row.getString("cron_exp")).getNextTickOrNone(minute))
                }).put("UPDATE qross_jobs SET next_tick=$next_tick WHERE id=$job_id")
        
        // ----- dependencies -----
        
        //get all new tasks
        dh.openDefault()
            .get("SELECT A.id AS task_id, A.job_id, A.task_time, A.status, B.dependencies FROM qross_tasks A INNER JOIN qross_jobs B ON A.job_id=B.id AND B.enabled='true' WHERE A.status='new'")
                .cache("tasks")
        
        //delete old dependencies first - comment on 20180512
        //dh.openCache()
        //    .get("SELECT task_id FROM tasks WHERE dependencies='yes'")
        //        .put("DELETE FROM qross_tasks_dependencies WHERE task_id=?")
        
        //get all dependencies
        dh.openCache()
            .get("SELECT DISTINCT job_id FROM tasks WHERE dependencies='yes'")
                .flat(table => DataRow("job_ids" -> (if (table.nonEmpty) table.mkString(",", "job_id") else "0")))
        dh.openDefault()
            .pass("SELECT id AS dependency_id, job_id, dependency_moment, dependency_type, dependency_value FROM qross_jobs_dependencies WHERE job_id IN (#job_ids)")
                .cache("dependencies")
        
        //generate dependencies
        dh.openCache()
            .get("SELECT A.job_id, A.task_id, A.task_time, B.dependency_id, B.dependency_moment, B.dependency_type, B.dependency_value FROM tasks A INNER JOIN dependencies B ON A.job_id=B.job_id")
                .table("job_id" -> DataType.INTEGER,
                        "task_id" -> DataType.INTEGER,
                        "dependency_id" -> DataType.INTEGER,
                        "dependency_moment" -> DataType.TEXT,
                        "dependency_type" -> DataType.TEXT,
                        "dependency_value" -> DataType.TEXT) (row => {
                            val table = DataTable()
                            TaskDependency.parseDependencyValue(row.getString("job_id"), row.getString("task_id"), row.getString("dependency_value"), row.getString("task_time"))
                                .foreach(value =>
                                    table.insertRow("job_id" -> row.getInt("job_id"),
                                        "task_id" -> row.getLong("task_id"),
                                        "dependency_id" -> row.getInt("dependency_id"),
                                        "dependency_moment" -> row.getString("dependency_moment"),
                                        "dependency_type" -> row.getString("dependency_type"),
                                        "dependency_value" -> value)
                                )
                            table
                }).put("INSERT INTO qross_tasks_dependencies (job_id, task_id, dependency_id, dependency_moment, dependency_type, dependency_value) VALUES (?, ?, ?, ?, ?, ?)")
        
        // ---------- DAGs ----------
        
        //delete old DAGs - comment on 20180512
        //dh.openCache()
        //    .get("SELECT task_id FROM tasks")
        //        .put("DELETE FROM qross_tasks_dags WHERE task_id=?;")
        
        //get all DAGs
        dh.openCache()
            .get("SELECT DISTINCT job_id FROM tasks")
                .flat(table => DataRow("job_ids" -> (if (table.nonEmpty) table.mkString(",", "job_id") else "0")))
        dh.openDefault()
            .pass("SELECT id AS command_id, job_id, upstream_ids FROM qross_jobs_dags WHERE job_id IN (#job_ids)")
                .cache("dags")
        
        //generate DAGs
        dh.openCache()
            .get("SELECT A.job_id, A.task_id, B.command_id, B.upstream_ids FROM tasks A INNER JOIN dags B ON A.job_id=B.job_id")
                .put("INSERT INTO qross_tasks_dags (job_id, task_id, command_id, upstream_ids) VALUES (?, ?, ?, ?)")
        
        //update tasks status
        dh.openCache()
            .get("SELECT (CASE dependencies WHEN 'yes' THEN 'initialized' ELSE 'ready' END) AS status, task_id FROM tasks")
                .put("UPDATE qross_tasks SET status=?, update_time=NOW() WHERE id=?")
        
        dh.openCache()
            .get("SELECT A.task_id FROM tasks A LEFT JOIN dags B ON A.job_id=B.job_id WHERE B.job_id IS NULL")
                .put("UPDATE qross_tasks SET status='miss_command', update_time=NOW() WHERE id=?")
        
        //send initialized tasks to checker, and send ready tasks to starter
        val prepared = dh.openDefault().executeDataTable("SELECT A.id AS task_id, A.status FROM qross_tasks A INNER JOIN qross_jobs B ON A.job_id=B.id AND B.enabled='true' WHERE A.status='initialized' OR A.status='ready'")
        
        //beat
        dh.openDefault().executeNonQuery("UPDATE qross_keeper_beats SET last_beat_time=NOW() WHERE actor_name='TaskProducer';")
        writeMessage("TaskProducer beat!")
        
        dh.close()
    
        prepared
    }
    
    def restartTask(taskId: Long, option: String): String = {
    
        //Reset task status to RESTARTING in master
        //Reset action status to WAITING
        //Return status: initialized or ready
    
        //UPDATE qross_tasks SET status=''restarting'',start_time=NULL,finish_time=NULL,spent=NULL,update_time=NOW() WHERE id=#{taskId};
        
        //INSERT INTO qross_message_box (message_type, message_key, message_text) VALUES ('TASK', 'RESTART', 'WHOLE@69');
        //INSERT INTO qross_message_box (message_type, message_key, message_text) VALUES ('TASK', 'RESTART', '3,4,5@69');
        //INSERT INTO qross_message_box (message_type, message_key, message_text) VALUES ('TASK', 'RESTART', '^EXCEPTIONAL@595052');
        
        //A. WHOLE: Restart whole task on FINISHED or INCORRECT or FAILED -  reset dependencies，reset dags
        //    option = WHOLE
        //B. ANY: Restart from one or more DONE action on FINISHED/FAILED/INCORRECT - keep dependencies, renew dags
        //        update all dependencies of the action to empty
        //   option =  ^CommandIDs - ^1,2,3,4,5
        //C. EXCEPTIONAL: Restart from all EXCEPTIONAL action on FAILED - keep dependencies, keep dags
        //   option = ^EXCEPTIONAL
        //D. PARTIAL: Restart one or more DONE action only on FINISHED/FAILED/INCORRECT -  keep dependencies, keep dags
        //   option = Commands - 1,2,3,4,5
        
        //restartMode
        val WHOLE = 1
        val PARTIAL = 2
        val EXCEPTIONAL = 3
        val ANY = 4
        
        val restartMode = if (option.toUpperCase() == "WHOLE") {
            WHOLE
        }
        else if (option.startsWith("^")) {
            if (option.toUpperCase() == "^EXCEPTIONAL") {
                EXCEPTIONAL
            }
            else {
                ANY
            }
        }
        else {
            PARTIAL
        }
        
        val dh = new DataHub()
    
        val row = dh.executeDataRow(s"SELECT A.job_id, A.status, B.dependencies FROM qross_tasks A INNER JOIN qross_jobs B ON A.id=$taskId AND A.job_id=B.id")
        var status = row.getString("status")
        val jobId = row.getInt("job_id")
        val dependencies = row.getBoolean("dependencies")
        
        if (status == TaskStatus.RESTARTING) {
            //clear all dependencies first
            if (restartMode == WHOLE && dependencies) {
                dh.executeNonQuery(s"DELETE FROM qross_tasks_dependencies WHERE task_id=$taskId")
            
                //generate dependencies
                dh.get(s"""SELECT A.task_time, B.dependency_id, B.dependency_moment, B.dependency_type, B.dependency_value
                    FROM (SELECT id, job_id, task_time FROM qross_tasks WHERE id=$taskId) A
                     INNER JOIN qross_jobs_dependencies B ON B.job_id=$jobId AND A.job_id=B.job_id""")
                    .table("job_id" -> DataType.INTEGER,
                        "task_id" -> DataType.INTEGER,
                        "dependency_id" -> DataType.INTEGER,
                        "dependency_moment" -> DataType.TEXT,
                        "dependency_type" -> DataType.TEXT,
                        "dependency_value" -> DataType.TEXT)(row => {
                        val table = DataTable()
                        TaskDependency.parseDependencyValue(jobId.toString, taskId.toString, row.getString("dependency_value"), row.getString("task_time"))
                            .foreach(value =>
                                table.insertRow("job_id" -> jobId,
                                    "task_id" -> taskId,
                                    "dependency_id" -> row.getInt("dependency_id"),
                                    "dependency_moment" -> row.getString("dependency_moment"),
                                    "dependency_type" -> row.getString("dependency_type"),
                                    "dependency_value" -> value)
                            )
                        table
                    }).put("INSERT INTO qross_tasks_dependencies (job_id, task_id, dependency_id, dependency_moment, dependency_type, dependency_value) VALUES (?, ?, ?, ?, ?, ?)")
            }
    
            restartMode match {
                case WHOLE =>
                    //clear dags
                    dh.executeNonQuery(s"DELETE FROM qross_tasks_dags WHERE task_id=$taskId")
                    //generate dags
                    dh.get(s"SELECT A.job_id, A.id AS task_id, B.id AS command_id, B.upstream_ids FROM qross_tasks A INNER JOIN qross_jobs_dags B ON B.job_id=$jobId AND A.id=$taskId AND A.job_id=B.job_id")
                        .put("INSERT INTO qross_tasks_dags (job_id, task_id, command_id, upstream_ids) VALUES (?, ?, ?, ?)")
                case PARTIAL => dh.executeNonQuery(s"UPDATE qross_tasks_dags SET status='waiting' WHERE task_id=$taskId AND command_id IN ($option)")
                case EXCEPTIONAL => dh.executeNonQuery(s"UPDATE qross_tasks_dags SET status='waiting' WHERE task_id=$taskId AND status IN ('exceptional', 'timeover')")
                case ANY =>
                    //refresh upstream_ids
                    dh.get(s"SELECT id, upstream_ids FROM qross_jobs_dags WHERE job_id=$jobId")
                        .put(s"UPDATE qross_tasks_dags SET upstream_ids='#upstream_ids',status='waiting' WHERE task_id=$taskId AND command_id=#id")
                    val commandIds = option.drop(1)
                    while (dh.get(s"SELECT id, command_id FROM qross_tasks_dags WHERE task_id=$taskId AND upstream_ids='' AND status='waiting' AND command_id NOT IN ($commandIds)").nonEmpty) {
                        dh.put(s"UPDATE qross_tasks_dags SET upstream_ids=REPLACE(upstream_ids, '(#command_id)', '') WHERE task_id=$taskId")
                            .put("UPDATE qross_tasks_dags SET status='done' WHERE id=#id")
                    }
            }
            
            if (restartMode == WHOLE && dependencies) {
                dh.executeNonQuery(s"UPDATE qross_tasks SET status='initialized' WHERE id=$taskId")
                status = TaskStatus.INITIALIZED
            } else {
                dh.executeNonQuery(s"UPDATE qross_tasks SET status='ready' WHERE id=$taskId")
                status = TaskStatus.READY
            }
        }
        
        dh.close()
        
        TaskRecord.of(jobId, taskId).debug(s"Task $taskId of job $jobId restart with option $option.")
        
        status
    }
    
    //TaskChecker
    def checkTaskDependencies(taskId: Long): Boolean = {
        val dh = new DataHub()
        
        //update task status to ready if all dependencies are ready
        dh.executeNonQuery(s"UPDATE qross_tasks SET status='ready', update_time=NOW() WHERE id=$taskId AND NOT EXISTS (SELECT task_id FROM qross_tasks_dependencies WHERE task_id=$taskId AND dependency_moment='before' AND ready='no')")
        
        //check dependencies
        dh.openDefault()
            .get(
                s"""SELECT A.id, A.task_id, A.dependency_type, A.dependency_value, A.ready, B.task_time, A.job_id
                    FROM (SELECT id, job_id, task_id, dependency_type, dependency_value, ready FROM qross_tasks_dependencies
                        WHERE task_id=$taskId AND dependency_moment='before' AND ready='no') A
                    INNER JOIN (SELECT id, task_time FROM qross_tasks where id=$taskId) B ON A.task_id=B.id""")
            .foreach(row => {
                val result = TaskDependency.check(row.getString("dependency_type"), row.getString("dependency_value"))
                row.set("ready", result._1)
                row.set("dependency_value", result._2)
                TaskRecord.of(row.getInt("job_id"), taskId).log(s"Task $taskId dependency ${row.getLong("id")} of job ${row.getInt("job_id")} is ${if (result._1 == "no") "not " else ""}ready.")
            }).cache("tasks")
            
        //update status and others after checking
        dh.openCache().saveAsDefault()
            .get("SELECT id FROM tasks WHERE ready='yes'")
                .put("UPDATE qross_tasks_dependencies SET ready='yes', update_time=NOW() WHERE id=#id")
            .get("SELECT dependency_value, id FROM tasks WHERE ready='no'")
                .put("UPDATE qross_tasks_dependencies SET dependency_value=?, retry_times=retry_times+1, update_time=NOW() WHERE id=?")
            .get("SELECT DISTINCT task_id FROM tasks WHERE NOT EXISTS (SELECT id FROM tasks WHERE ready='no')")
                .put("UPDATE qross_tasks SET status='ready', update_time=NOW() WHERE id=?")
        
        //update status if retry reached upper limit
        val limit = dh.openDefault().executeDataRow(
            s"""SELECT A.task_id, C.task_time, A.retry_times, B.retry_limit, D.title, A.job_id, D.owner, D.mail_notification, D.mail_master_on_exception
                FROM (SELECT task_id, job_id, dependency_id, retry_times FROM qross_tasks_dependencies WHERE task_id=$taskId AND dependency_moment='before' AND ready='no') A
                INNER JOIN qross_jobs_dependencies B ON A.dependency_id=B.id AND B.retry_limit>0 AND A.retry_times>=B.retry_limit
                INNER JOIN (SELECT id, task_time FROM qross_tasks WHERE id=$taskId AND status='initialized') C ON A.task_id=C.id
                INNER JOIN qross_jobs D ON A.job_id=D.id""")
                
        if (limit.nonEmpty) {
            dh.executeNonQuery(s"UPDATE qross_tasks SET status='checking_limit', checked='no', update_time=NOW() WHERE id=$taskId")
            if (Global.EMAIL_NOTIFICATION) {
                if (limit.getBoolean("mail_notification") && limit.getString("owner") != "") {
                    OpenResourceFile("/templates/checking_limit.html")
                        .replace("${status}", "CHECKING_LIMIT")
                        .replaceWith(limit)
                        .writeEmail(s"CHECKING_LIMIT: ${limit.getString("title")} ${limit.getString("task_time")} - TaskID: ${limit.getString("task_id")}")
                        .to(limit.getString("owner"))
                        .cc(if (limit.getBoolean("mail_master_on_exception")) Global.MASTER_USER_GROUP else "")
                        .send()
                }
            }
            
            TaskRecord.of(limit.getInt("job_id"), taskId).warn(s"Task $taskId of job ${limit.getInt("job_id")} reached upper limit of checking limit.")
        }
        
        val result = dh.executeDataRow(s"SELECT job_id, status FROM qross_tasks WHERE id=$taskId")
        
        dh.close()
        
        TaskRecord.of(result.getInt("job_id"), taskId).log(s"Task $taskId of job ${result.getInt("job_id")} status is ${if (result.getString("status") == "initialized") "not" else ""} ready after pre-dependencies checking.")
        
        result.getString("status") == "ready"
    }
    
    //TaskStarter - beat()
    /*
    def getManualCommandsToExecute(tick: String): DataTable = {
        val minute = DateTime(tick)
        val ds = new DataSource()
        //commands
        val executable = ds.executeDataTable(commandsBaseSQL
            + " WHERE B.status IN ('finished', 'failed', 'incorrect') AND A.status IN ('restarting', 'manual')")
    
        writeMessage("TaskStarter beat!")
        ds.executeNonQuery(s"UPDATE qross_keeper_beats SET last_beat_time='${minute.getString("yyyy-MM-dd HH:mm:ss")}' WHERE actor_name='TaskStarter';")
        ds.close()
    
        executable
    }
    def checkOvertimeOfActions(tick: String): Unit = {
        val minute = DateTime(tick)
        val dh = new DataHub()
        
        dh.get("SELECT A.id AS action_id, A.job_id, A.task_id, A.command_id, B.command_text, B.overtime, C.title, C.owner, C.mail_notification, C.mail_master_on_exception, D.task_time FROM qross_tasks_dags A INNER JOIN qross_jobs_dags B ON A.status='running' AND A.job_id=B.job_id AND B.overtime>0 AND TIMESTAMPDIFF(SECOND, A.update_time, NOW())>B.overtime INNER JOIN qross_jobs C ON A.job_id=C.id AND C.enabled='true' INNER JOIN qross_tasks D ON A.task_id=D.id")
        if (dh.nonEmpty) {
            dh.put("UPDATE qross_tasks_dags SET status='timeout', update_time=NOW() WHERE id=#action_id")
                .put("UPDATE qross_tasks SET status='timeout', checked='no', update_time=NOW() WHERE id=#task_id")
    
            if (Global.EMAIL_NOTIFICATION) {
                dh.foreach(row => {
                    if (row.getBoolean("mail_notification") && row.getString("owner", "") != "") {
                        OpenResourceFile("/templates/timeout.html")
                            .replaceWith(row)
                            .writeEmail(s"TIMEOUT: ${row.getString("title")} ${row.getString("task_time")} - TaskID: ${row.getString("task_id")}")
                            .to(row.getString("owner"))
                            .cc(if (row.getBoolean("mail_master_on_exception")) Global.MASTER_USER_GROUP else "")
                            .send()
                    }
                })
            }
        }
    
        writeMessage("TaskStarter beat!")
        dh.executeNonQuery(s"UPDATE qross_keeper_beats SET last_beat_time='${minute.getString("yyyy-MM-dd HH:mm:ss")}' WHERE actor_name='TaskStarter';")
        dh.close()
    }
    */
    
    //TaskStarter - execute()
    def getTaskCommandsToExecute(taskId: Long, status: String): DataTable = synchronized {
        val ds = new DataSource()
        
        //get
        if (status == TaskStatus.READY) {
            //job enabled = true
            //get job id
            //get job concurrent_limit by job id
            //get concurrent task count by job id
            //update tasks if meet the condition - concurrent_limit=0 OR concurrent < concurrent_limit
            
            val job = ds.executeDataRow(s"SELECT id AS job_id, concurrent_limit, enabled FROM qross_jobs WHERE id=(SELECT job_id FROM qross_tasks WHERE id=$taskId)")
            if (job.getBoolean("enabled")) {
                val concurrentLimit = job.getInt("concurrent_limit")
                val jobId = job.getInt("job_id")
                if (concurrentLimit == 0 || ds.executeDataRow(s"SELECT COUNT(0) AS concurrent FROM qross_tasks WHERE job_id=$jobId AND status='executing'").getInt("concurrent") < concurrentLimit) {
                    ds.executeNonQuery(s"UPDATE qross_tasks SET status='executing', start_time=NOW(), update_time=NOW() WHERE id=$taskId AND status='ready'")
                    TaskRecord.of(jobId, taskId).log(s"Task $taskId of job $jobId is executing.")
                }
                else {
                    TaskRecord.of(jobId, taskId).warn(s"Concurrent reach upper limit of Job $jobId for Task $taskId")
                }
            }
        }
    
        val executable = ds.executeDataTable(
            s"""SELECT A.action_id, A.job_id, A.task_id, A.command_id, B.task_time, C.command_type, C.command_text,
                         C.overtime, D.title, D.owner, D.mail_notification, D.mail_master_on_exception, C.retry_limit
                         FROM (SELECT id AS action_id, job_id, task_id, command_id FROM qross_tasks_dags WHERE task_id=$taskId AND status='waiting' AND upstream_ids='') A
                         INNER JOIN (SELECT id, task_time FROM qross_tasks WHERE id=$taskId AND status='executing') B ON A.task_id=B.id
                         INNER JOIN qross_jobs_dags C ON A.command_id=C.id
                         INNER JOIN qross_jobs D ON A.job_id=D.id""")
        //prepare to run command - start time point
        ds.tableUpdate("UPDATE qross_tasks_dags SET start_time=NOW(), status='queuing', update_time=NOW() WHERE id=#action_id", executable)
        
        ds.close()
        
        executable
    }
    
    //TaskExecutor
    def executeTaskCommand(taskCommand: DataRow): Long = {
    
        val dh = new DataHub()
        
        val actionId = taskCommand.getLong("action_id")
        val jobId = taskCommand.getInt("job_id")
        val taskId = taskCommand.getLong("task_id")
        val taskTime = taskCommand.getString("task_time")
        val commandId = taskCommand.getInt("command_id")
        val retryLimit = taskCommand.getInt("retry_limit")
        val overtime = taskCommand.getInt("overtime")
        val title = taskCommand.getString("title")
        val mailNotifaction = taskCommand.getBoolean("mail_notification")
        val owner = taskCommand.getString("owner")
        
        var commandText = taskCommand.getString("command_text")
        commandText = commandText.replace("${jobId}", s"$jobId")
        commandText = commandText.replace("${taskId}", s"$taskId")
        commandText = commandText.replace("${taskTime}", taskTime)
        commandText = commandText.replace("${commandId}", s"$commandId")
        commandText = commandText.replace("${actionId}", s"$actionId")
        commandText = commandText.replace("%QROSS_VERSION", Global.QROSS_VERSION)
        commandText = commandText.replace("%JAVA_BIN_HOME", Global.JAVA_BIN_HOME)
        commandText = commandText.replace("%QROSS_HOME", Global.QROSS_HOME)
        
        //replace datetime format
        while (commandText.contains("${") && commandText.contains("}")) {
            val ahead = commandText.substring(0, commandText.indexOf("${"))
            var format = commandText.substring(commandText.indexOf("${") + 2)
            val latter = format.substring(format.indexOf("}") + 1)
            format = format.substring(0, format.indexOf("}"))
            commandText = ahead + DateTime(taskTime).sharp(format).mkString(",") + latter
        }

        dh.executeNonQuery(s"UPDATE qross_tasks_dags SET status='running', run_time=NOW(), waiting=TIMESTAMPDIFF(SECOND, start_time, NOW()), update_time=NOW() WHERE id=$actionId")
        dh.executeNonQuery(s"DELETE FROM qross_tasks_logs WHERE task_id=$taskId AND command_id=$commandId") //clear logs of old actions too
        
        var retry = -1
        var exitValue = 1
        var next = false
        
        //LET's GO!
        val logger = TaskRecord.of(jobId, taskId).run(commandId, actionId)
        logger.debug(s"START action $actionId - command $commandId of task $taskId - job $jobId: $commandText")
        
        do {
            if (retry > 0) logger.debug(s"Action $actionId - command $commandId of task $taskId - job $jobId: retry $retry of limit $retryLimit")
            val start = System.currentTimeMillis()
            var timeout = false
            
            val process = commandText.run(ProcessLogger(out => {
                logger.out(out)
            }, err => {
                logger.err(err)
            }))
    
            while (process.isAlive()) {
                //if timeout
                if (overtime > 0 && (System.currentTimeMillis() - start) / 1000 > overtime) {
                    process.destroy() //kill it
                    timeout = true
                    
                    logger.warn(s"Action $actionId - command $commandId of task $taskId - job $jobId is TIMEOUT: $commandText")
                }
                
                Timer.sleep(1)
            }
    
            exitValue = process.exitValue()
            if (timeout) exitValue = -1
            
            retry += 1
        }
        while (retry < retryLimit && exitValue != 0)
    
        logger.debug(s"FINISH action $actionId - command $commandId of task $taskId - job $jobId with exitValue $exitValue and status ${if (exitValue == 0) "SUCCESS" else if (exitValue > 0)  "FAILURE" else "TIMEOUT/INTERRUPTED" }")
    
        exitValue match {
            //finished
            case 0 =>
                //update DAG status
                dh.executeNonQuery(s"UPDATE qross_tasks_dags SET status='done', elapsed=TIMESTAMPDIFF(SECOND, run_time, NOW()), finish_time=NOW(), retry_times=$retry, update_time=NOW() WHERE id=$actionId")
                //update DAG dependencies
                dh.executeNonQuery(s"UPDATE qross_tasks_dags SET upstream_ids=REPLACE(upstream_ids, '($commandId)', ''), update_time=NOW() WHERE task_id=$taskId AND status='waiting' AND POSITION('($commandId)' IN upstream_ids)>0;")
    
                //if continue
                next = dh.executeExists(s"SELECT id FROM qross_tasks_dags WHERE task_id=$taskId AND status='waiting' LIMIT 1")
                if (!next) {
                    //meet: no waiting action, no running action
                    //action status: all done - task status: executing -> finished
                    //if exceptional action exists - task status: executing, finished -> failed
        
                    //update task status if all finished
                    dh.executeNonQuery(s"UPDATE qross_tasks SET finish_time=NOW(), spent=TIMESTAMPDIFF(SECOND, start_time, NOW()), status='finished', checked='', update_time=NOW() WHERE id=$taskId AND NOT EXISTS (SELECT id FROM qross_tasks_dags WHERE task_id=$taskId AND status!='done')")
                    //dh.executeNonQuery(s"UPDATE qross_tasks SET finish_time=NOW(), status='failed', checked='no', update_time=NOW() WHERE id=$taskId AND status IN ('executing', 'finished') AND NOT EXISTS (SELECT id FROM qross_tasks_dags WHERE task_id=$taskId AND status='running') AND EXISTS(SELECT id FROM qross_tasks_dags WHERE task_id=$taskId AND status='exceptional')")  //6.12
        
                    //check "after" dependencies
                    dh.get(s"SELECT A.id, A.task_id, A.dependency_type, A.dependency_value, A.ready, B.task_time FROM qross_tasks_dependencies A INNER JOIN qross_tasks B ON A.job_id=B.job_id WHERE B.status='finished' AND A.task_id=$taskId AND A.dependency_moment='after' AND A.ready='no'")
                        .foreach(row => {
                            val result = TaskDependency.check(row.getString("dependency_type"), row.getString("dependency_value"))
                            row.set("ready", result._1)
                            row.set("dependency_value", result._2)
                        }).put("UPDATE qross_tasks_dependencies SET ready=$ready, dependency_value=$dependency_value, update_time=NOW() WHERE id=$id")
        
                    //update tasks status if incorrect
                    dh.executeNonQuery(s"UPDATE qross_tasks SET status='incorrect', checked='no', update_time=NOW() WHERE id=$taskId AND status='finished' AND EXISTS(SELECT id FROM qross_tasks_dependencies WHERE task_id=$taskId AND dependency_moment='after' AND ready='no')")
                }
            //timeout
            case -1 =>
                dh.executeNonQuery(s"UPDATE qross_tasks_dags SET status='timeover', retry_times=$retry, update_time=NOW() WHERE id=$actionId")
                dh.executeNonQuery(s"UPDATE qross_tasks SET status='timeout', checked='no', update_time=NOW() WHERE id=$taskId")
            //failed
            case _ =>
                dh.executeNonQuery(s"UPDATE qross_tasks_dags SET status='exceptional', retry_times=$retry, update_time=NOW() WHERE id=$actionId")
                dh.executeNonQuery(s"UPDATE qross_tasks SET finish_time=NOW(), status='failed', checked='no', update_time=NOW() WHERE id=$taskId")
        }

        val status = dh.executeSingleValue(s"SELECT status FROM qross_tasks WHERE id=$taskId").getOrElse("miss")
        //send notification mail if failed or timeout or incorrect
        if (Global.EMAIL_NOTIFICATION && taskCommand.getBoolean("mail_notification") && owner != "") {
            if (status == "failed" || status == "timeout" || status == "incorrect") {
                OpenResourceFile("/templates/failed_incorrect.html")
                    .replace("${status}", status.toUpperCase())
                    .replaceWith(taskCommand)
                    .replace("${logs}", TaskLogger.toHTML(dh.executeDataTable(s"SELECT CAST(create_time AS CHAR) AS create_time, log_type, log_text FROM qross_tasks_logs WHERE task_id=$taskId AND command_id=$commandId ORDER BY create_time ASC")))
                    .writeEmail(s"${status.toUpperCase()}: ${taskCommand.getString("title")} $taskTime - TaskID: $taskId")
                    .to(owner)
                    .cc(if (taskCommand.getBoolean("mail_master_on_exception")) Global.MASTER_USER_GROUP else "")
                    .send()
            }
        }
        //incorrect
        if (status == "incorrect") {
            logger.warn(s"Action $actionId - command $commandId of task $taskId - job $jobId is INCORRECT: $commandText")
        }
        
        dh.close()
    
        if (next) {
            //return
            taskId
        } else {
            TaskRecord.of(jobId, taskId).debug(s"Task $taskId of job $jobId finish with status ${status.toUpperCase}.")
            //clear logger
            TaskRecord.dispose(taskId)
            //return
            0
        }
    }
}