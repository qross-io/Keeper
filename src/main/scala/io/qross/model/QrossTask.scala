package io.qross.model

import io.qross.util.Output.writeMessage
import io.qross.util._
import scala.sys.process._
import scala.util.{Success, Try}

object QrossTask {
    
    def main(args: Array[String]): Unit = {
        //println(DateTime("2018-03-16 19:21:00"))
        //CronExp.getTicks("0 * * * * ? *", DateTime("2018-03-16 19:21:00"), DateTime("2018-03-17 23:37:00"))
          //  .foreach(println)
        println(CronExp("0 0/20 * * * ? *").getNextTickOrNone(DateTime.now))
    }
    
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
            .pass("SELECT job_id, dependency_moment, dependency_type, dependency_value FROM qross_jobs_dependencies WHERE job_id IN (#job_ids)")
                .cache("dependencies")
        
        //generate dependencies
        dh.openCache()
            .get("SELECT A.job_id, A.task_id, A.task_time, B.dependency_moment, B.dependency_type, B.dependency_value FROM tasks A INNER JOIN dependencies B ON A.job_id=B.job_id")
                .table("job_id" -> DataType.INTEGER,
                        "task_id" -> DataType.INTEGER,
                       "dependency_moment" -> DataType.TEXT,
                        "dependency_type" -> DataType.TEXT,
                        "dependency_value" -> DataType.TEXT) (row => {
                            val table = DataTable()
                            TaskDependency.parseDependencyValue(row.getString("job_id"), row.getString("task_id"), row.getString("dependency_value"), row.getString("task_time"))
                                .foreach(value =>
                                    table.insertRow("job_id" -> row.getInt("job_id"),
                                        "task_id" -> row.getLong("task_id"),
                                        "dependency_moment" -> row.getString("dependency_moment"),
                                        "dependency_type" -> row.getString("dependency_type"),
                                        "dependency_value" -> value)
                                )
                            table
                }).put("INSERT INTO qross_tasks_dependencies (job_id, task_id, dependency_moment, dependency_type, dependency_value) VALUES (?, ?, ?, ?, ?)")
        
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
        
        dh.openDefault().executeNonQuery("UPDATE qross_keeper_beats SET last_beat_time=NOW() WHERE actor_name='TaskProducer';")
        writeMessage("TaskProducer beat!")
        
        dh.close()
    
        prepared
    }
    
    def restartTask(taskId: Long, option: String): String = {
    
        //Reset task status to RESTARTING in master
        //Reset action status to WAITING
        //Return status: initialized or ready
    
        //UPDATE qross_tasks SET status=''restarting'',start_time=NULL,finish_time=NULL,spent=NULL,retry_times=0,update_time=NOW() WHERE id=#{taskId};
    
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
        
        val restartMode = if (option == "WHOLE") {
            WHOLE
        }
        else if (option.startsWith("^")) {
            if (option == "^EXCEPTIONAL") {
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
            //clear first
            if (restartMode == WHOLE && dependencies) {
                dh.executeNonQuery(s"DELETE FROM qross_tasks_dependencies WHERE task_id=$taskId")
            
                //generate dependencies
                dh.get(s"SELECT A.task_time, B.dependency_moment, B.dependency_type, B.dependency_value FROM qross_tasks A INNER JOIN qross_jobs_dependencies B ON B.job_id=$jobId AND A.id=$taskId AND A.job_id=B.job_id")
                    .table("job_id" -> DataType.INTEGER,
                        "task_id" -> DataType.INTEGER,
                        "dependency_moment" -> DataType.TEXT,
                        "dependency_type" -> DataType.TEXT,
                        "dependency_value" -> DataType.TEXT)(row => {
                        val table = DataTable()
                        TaskDependency.parseDependencyValue(jobId.toString, taskId.toString, row.getString("dependency_value"), row.getString("task_time"))
                            .foreach(value =>
                                table.insertRow("job_id" -> jobId,
                                    "task_id" -> taskId,
                                    "dependency_moment" -> row.getString("dependency_moment"),
                                    "dependency_type" -> row.getString("dependency_type"),
                                    "dependency_value" -> value)
                            )
                        table
                    }).put("INSERT INTO qross_tasks_dependencies (job_id, task_id, dependency_moment, dependency_type, dependency_value) VALUES (?, ?, ?, ?, ?)")
            }
    
            restartMode match {
                case WHOLE =>
                    //clear dags
                    dh.executeNonQuery(s"DELETE FROM qross_tasks_dags WHERE task_id=$taskId")
                    //generate dags
                    dh.get(s"SELECT A.job_id, A.id AS task_id, B.id AS command_id, B.upstream_ids FROM qross_tasks A INNER JOIN qross_jobs_dags B ON B.job_id=$jobId AND A.id=$taskId AND A.job_id=B.job_id")
                        .put("INSERT INTO qross_tasks_dags (job_id, task_id, command_id, upstream_ids) VALUES (?, ?, ?, ?)")
                case PARTIAL => dh.executeNonQuery(s"UPDATE qross_tasks_dags SET status='waiting' WHERE task_id=$taskId AND command_id IN ($option)")
                case EXCEPTIONAL => dh.executeNonQuery(s"UPDATE qross_tasks_dags SET status='waiting' WHERE task_id=$taskId AND status='exceptional'")
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
        status
    }
    
    //TaskChecker
    def checkTaskDependencies(taskId: Long): Boolean = {
        val dh = new DataHub()
        
        //update task status to ready if all dependencies are ready
        dh.executeNonQuery(s"UPDATE qross_tasks SET status='ready', update_time=NOW() WHERE id=$taskId AND NOT EXISTS (SELECT task_id FROM qross_tasks_dependencies WHERE task_id=$taskId AND dependency_moment='before' AND ready='no')")
        
        //check dependencies
        dh.openDefault()
            .get(s"SELECT A.id, A.task_id, A.dependency_type, A.dependency_value, A.ready, B.task_time FROM qross_tasks_dependencies A INNER JOIN qross_tasks B ON A.job_id=B.job_id INNER JOIN qross_jobs C ON A.job_id=C.id AND C.enabled='true' WHERE A.task_id=$taskId AND A.dependency_moment='before' AND A.ready='no'")
            .foreach(row => {
                val result = TaskDependency.check(row.getString("dependency_type"), row.getString("dependency_value"))
                row.set("ready", result._1)
                row.set("dependency_value", result._2)
            }).cache("tasks")
            
        //update status and others after checking
        dh.openCache().saveAsDefault()
            .get("SELECT id FROM tasks WHERE ready='yes'")
                .put("UPDATE qross_tasks_dependencies SET ready='yes', update_time=NOW() WHERE id=?")
            .get("SELECT dependency_value, id FROM tasks WHERE ready='no'")
                .put("UPDATE qross_tasks_dependencies SET dependency_value=?, update_time=NOW() WHERE id=?")
            .get("SELECT DISTINCT task_id FROM tasks WHERE ready='no'")
                .put("UPDATE qross_tasks SET retry_times=retry_times+1, update_time=NOW() WHERE id=?")
            .get("SELECT DISTINCT task_id FROM tasks WHERE NOT EXISTS (SELECT id FROM tasks WHERE ready='no')")
                .put("UPDATE qross_tasks SET status='ready', update_time=NOW() WHERE id=?")
        
        //update status if retry reached upper limit
        dh.openDefault()
            .get("SELECT A.id AS task_id, A.task_time, A.retry_times, B.title, A.job_id, B.owner, B.mail_notification, B.mail_master_on_exception FROM qross_tasks A INNER JOIN qross_jobs B ON A.job_id=B.id WHERE B.enabled='true' AND A.status='initialized' AND B.checking_retry_limit>0 AND A.retry_times>=B.checking_retry_limit")
            .put("UPDATE qross_tasks SET status='checking_limit', checked='no', update_time=NOW() WHERE id=#task_id")
        
        if (Global.EMAIL_NOTIFICATION) {
            dh.foreach(row => {
                if (row.getBoolean("mail_notification") && row.getString("owner", "") != "") {
                    OpenResourceFile("/templates/checking_limit.html")
                        .replace("${status}", "CHECKING_LIMIT")
                        .replace("${retry_times}", row.getString("retry_times"))
                        .replaceWith(row)
                        .writeEmail(s"NOTIFICATION: ${row.getString("title")} ${row.getString("task_time")} CHECKING_LIMIT - TaskID: ${row.getString("task_id")}")
                        .to(row.getString("owner"))
                        .cc(if (row.getBoolean("mail_master_on_exception")) Global.MASTER_USER_GROUP else "")
                        .send()
                }
            })
        }
    
        writeMessage("TaskChecker beat!")
        dh.openDefault().executeNonQuery("UPDATE qross_keeper_beats SET last_beat_time=NOW() WHERE actor_name='TaskChecker';")
        
        val status = dh.executeSingleValue(s"SELECT status FROM qross_tasks WHERE id=$taskId")
        
        dh.close()
        
        status match {
            case Some(value) => value.toString == "ready"
            case _ => false
        }
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
    } */
    def checkOvertimeOfActions(tick: String): Unit = {
        val minute = DateTime(tick)
        val dh = new DataHub()
        
        dh.get("SELECT A.id AS action_id, A.job_id, A.task_id, A.command_id, B.command_text, B.overtime, C.title, C.owner, C.mail_notification, C.mail_master_on_exception, D.task_time FROM qross_tasks_dags A INNER JOIN qross_jobs_dags ON A.status='running' AND A.job_id=B.job_id AND B.overtime>0 AND TIMESTAMPDIFF(SECOND, A.update_time, NOW())>B.overtime INNER JOIN qross_jobs C ON A.job_id=C.id AND C.enabled='yes' INNER JOIN qross_tasks D ON A.task_id=D.id")
        if (dh.nonEmpty) {
            dh.put("UPDATE qross_tasks_dags SET status='timeout', update_time=NOW() WHERE id=#id")
                .put("UPDATE qross_tasks SET status='timeout', checked='no', update_time=NOW() WHERE id=#task_id")
    
            if (Global.EMAIL_NOTIFICATION) {
                dh.foreach(row => {
                    if (row.getBoolean("mail_notification") && row.getString("owner", "") != "") {
                        OpenResourceFile("/templates/timeout.html")
                            .replaceWith(row)
                            .writeEmail(s"NOTIFICATION: ${row.getString("title")} ${row.getString("task_time")} TIMEOUT - TaskID: ${row.getString("task_id")}")
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
    
    //TaskStarter - execute()
    def getTaskCommandsToExecute(taskId: Long, status: String): DataTable = {
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
                if (concurrentLimit == 0 || ds.executeDataRow(s"SELECT COUNT(0) AS concurrent FROM qross_tasks WHERE job_id=${job.getString("job_id")} AND status='executing'").getInt("concurrent") < concurrentLimit) {
                    ds.executeNonQuery(s"UPDATE qross_tasks SET status='executing', start_time=NOW(), update_time=NOW() WHERE id=$taskId AND status='ready'")
                }
                else {
                    writeMessage(s"Concurrent reach upper limit of Job ${job.getInt("job_id")} for Task $taskId")
                }
            }
    
            /*
            //will lead to java.sql.SQLException: Table 'qross_tasks' is specified twice, both as a target for 'UPDATE' and as a separate source for data
            ds.executeNonQuery(s"UPDATE qross_tasks SET status='executing', start_time=NOW(), update_time=NOW() WHERE id=$taskId AND status='ready'" +
                " AND EXISTS(SELECT id FROM " +
                        "(SELECT A.id, B.concurrent_limit, " +
                            "(SELECT COUNT(0) FROM qross_tasks WHERE job_id=" +
                                s"(SELECT job_id FROM qross_tasks WHERE id=$taskId) AND status='executing') AS concurrent" +
                        s" FROM qross_tasks A INNER JOIN qross_jobs B ON A.job_id=B.id AND B.enabled='true' WHERE A.id=$taskId) C WHERE concurrent_limit=0 OR concurrent<concurrent_limit)")
            */
        }
    
        val executable = ds.executeDataTable("SELECT  A.action_id, A.job_id, A.task_id, A.command_id, B.task_time, C.command_type, C.command_text, D.title, D.owner, D.mail_notification, D.mail_master_on_exception, D.executing_retry_limit AS retry_limit " +
            s" FROM (SELECT id AS action_id, job_id, task_id, command_id FROM qross_tasks_dags WHERE task_id=$taskId AND status='waiting' AND upstream_ids='') A" +
            s" INNER JOIN (SELECT id, task_time FROM qross_tasks WHERE id=$taskId AND status='executing') B ON A.task_id=B.id" +
            " INNER JOIN qross_jobs_dags C ON A.command_id=C.id" +
            " INNER JOIN qross_jobs D ON A.job_id=D.id AND D.enabled='true'")
        
        //will not work correctly if reach concurrent limit
//        if (executable.isEmpty) {
//            //no command to execute (no command) - when hasn't setup dags - normally if will never occur
//            ds.executeNonQuery(s"UPDATE qross_tasks SET status='incorrect' WHERE id=$taskId")
//        }
        
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
        val retryLimit = taskCommand.getInt("executing_retry_limit")
        
        var commandText = taskCommand.getString("command_text")
        commandText = commandText.replace("${taskId}", s"$taskId")
        commandText = commandText.replace("${jobId}", s"$jobId")
        commandText = commandText.replace("${taskTime}", taskTime)
        commandText = commandText.replace("${commandId}", s"$commandId")
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

        dh.executeNonQuery(s"UPDATE qross_tasks_dags SET status='running', update_time=NOW() WHERE id=$actionId")
        dh.executeNonQuery(s"DELETE FROM qross_tasks_logs WHERE task_id=$taskId AND command_id=$commandId") //clear logs of old actions too
        writeMessage(s"START: $commandText")
    
        var retry = 0
        var exitValue = 1
        var next = false
        
        //LET's GO!
        val logger = TaskLogger(jobId, taskId, commandId, actionId)
        logger.log(s"START Command $commandId of Task $taskId: $commandText")
        
        do {
            if (retry > 0) logger.log(s"Retry $retry of limit $retryLimit")
            exitValue = commandText.!(ProcessLogger(out => {
                logger.log(out)
            }, err => {
                logger.err(err)
            }))
            retry += 1
        }
        while (retry < retryLimit && exitValue != 0)
    
        logger.log(s"FINISH command $commandId of task $taskId with exitValue $exitValue and status ${if (exitValue == 0) "SUCCESS" else "FAILURE" }")
        logger.close()
    
        if (exitValue == 0) {
            //update DAG status
            dh.executeNonQuery(s"UPDATE qross_tasks_dags SET status='done', update_time=NOW() WHERE id=$actionId")

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
                dh.executeNonQuery(s"UPDATE qross_tasks SET finish_time=NOW(), spent=TIMESTAMPDIFF(SECOND, start_time, NOW()), status='failed', checked='no', update_time=NOW() WHERE id=$taskId AND status IN ('executing', 'finished') AND NOT EXISTS (SELECT id FROM qross_tasks_dags WHERE task_id=$taskId AND status='running') AND EXISTS(SELECT id FROM qross_tasks_dags WHERE task_id=$taskId AND status='exceptional')")

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
        }
        else {
            dh.executeNonQuery(s"UPDATE qross_tasks_dags SET status='exceptional', update_time=NOW() WHERE id=$actionId")
            dh.executeNonQuery(s"UPDATE qross_tasks SET finish_time=NOW(), spent=TIMESTAMPDIFF(SECOND, start_time, NOW()), status='failed', checked='no', update_time=NOW() WHERE id=$taskId")
        }

        //send notification mail if failed or incorrect
        if (Global.EMAIL_NOTIFICATION && taskCommand.getBoolean("mail_notification")) {
            dh.get(s"SELECT status FROM qross_tasks WHERE id=$taskId AND (status='failed' OR status='incorrect')")
                .foreach(row => {
                    if (taskCommand.getString("owner", "") != "") {
                        OpenResourceFile("/templates/failed_incorrect.html")
                            .replace("${status}", row.getString("status").toUpperCase())
                            .replaceWith(taskCommand)
                            .replace("${logs}", TaskLogger.toHTML(dh.executeDataTable(s"SELECT CAST(create_time AS CHAR) AS create_time, log_type, log_text FROM qross_tasks_logs WHERE task_id=$taskId AND command_id=$commandId ORDER BY create_time ASC")))
                            .writeEmail(s"NOTIFICATION: ${taskCommand.getString("title")} $taskTime ${row.getString("status").toUpperCase()} - TaskID: $taskId")
                            .to(taskCommand.getString("owner"))
                            .cc(if (taskCommand.getBoolean("mail_master_on_exception")) Global.MASTER_USER_GROUP else "")
                            .send()
                    }
                })
        }
        
        writeMessage("TaskExecutor beat!")
        dh.openDefault().executeNonQuery("UPDATE qross_keeper_beats SET last_beat_time=NOW() WHERE actor_name='TaskExecutor';")
        
        dh.close()
    
        writeMessage(s"FINISH: $commandText with exitValue $exitValue and next is $next")
        
        if (next) taskId else 0
    }
}