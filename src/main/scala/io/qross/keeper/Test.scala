package io.qross.keeper

import java.sql.DriverManager

import io.qross.util._

object Test {
    def main(args: Array[String]): Unit = {

        val dh = new DataHub()

        dh.insertRow("job_ids" -> 0)
        dh.pass("SELECT id AS dependency_id, job_id, dependency_moment, dependency_type, dependency_value FROM qross_jobs_dependencies WHERE job_id IN (#job_ids)").show()

        dh.close()





        //val list = List[String]("1", "2", "3")

//        val dh = new DataHub()
//        dh.get("SELECT * FROM tc").cache("tc")
//        dh.openCache().executeDataTable("SELECT (CASE WHEN status='waiting' THEN 1 ELSE 0 END) AS key FROM tc").show()
//        dh.close()
        /*println(DateTime.now.getString("yyyyMMdd/HH"))

        //QrossTask.checkTaskDependencies(542973L)
        //println(DateTime.of(2018, 3, 1).toEpochSecond)

        HDFS.list(args(1)).foreach(hdfs => {
            val reader = new HDFSReader(hdfs.path)
            var line = ""
            var count = 0
            while(reader.hasNextLine) {
                line = reader.readLine
                count += 1
            }
            reader.close()

            println(hdfs.path + " # " + count)
        })
        */

        //val dh = new DataHub()
        //dh.get("SELECT id FROM qross_jobs")
        //    .pass("SELECT job_id, GROUP_CONCAT(CONCAT(id, ':', status, '@', task_time) ORDER BY id ASC SEPARATOR ',') AS status FROM (SELECT job_id, id, status, task_time FROM qross_tasks WHERE job_id=#id ORDER BY id DESC LIMIT 3) T GROUP BY job_id")
        //    .show()
            //.put("UPDATE qross_jobs SET recent_tasks_status='#status' WHERE id=#job_id")
        //dh.close()

//          .set("ALTER TABLE qross_jobs MODIFY COLUMN id INT")
//          .set("ALTER TABLE qross_jobs_dags MODIFY COLUMN id INT")
//          .set("ALTER TABLE qross_jobs_dependencies MODIFY COLUMN id INT")
//          .set("ALTER TABLE qross_tasks MODIFY COLUMN id BIGINT")
//          .set("ALTER TABLE qross_tasks_dags MODIFY COLUMN id BIGINT")
//          .set("ALTER TABLE qross_tasks_dependencies MODIFY COLUMN id BIGINT")
//          .set("ALTER TABLE qross_tasks_logs MODIFY COLUMN id BIGINT")

//        dh.open("mysql.qross_release").saveAs("mysql.qross")
//
//        var id = "0"
//        var continue = true
//        while (continue && dh.open("mysql.qross_release").get(s"SELECT id, job_id, task_id, command_id, action_id, log_type, log_text, create_time FROM qross_tasks_logs WHERE job_id>1 AND id>$id LIMIT 10000").nonEmpty) {
//            dh.put("INSERT INTO qross_tasks_logs (id, job_id, task_id, command_id, action_id, log_type, log_text, create_time) VALUES (?, ?, ?, ?, ?, ?, ?, ?)")
//
//            println(id)
//
//            dh.open("mysql.qross").executeSingleValue("SELECT id FROM qross_tasks_logs ORDER BY id DESC LIMIT 1") match {
//                case Some(v) => id = v
//                case None => continue = false
//            }
//        }

            //.get("SELECT id, title, job_type, owner, description, enabled, cron_exp, next_tick, dependencies, mail_notification, complement_missed_tasks, concurrent_limit, create_time, update_time, mail_master_on_exception, keep_x_task_records FROM qross_jobs WHERE id>1")
            //    .put("INSERT INTO qross_jobs (id, title, job_type, owner, description, enabled, cron_exp, next_tick, dependencies, mail_notification, complement_missed_tasks, concurrent_limit, create_time, update_time, mail_master_on_exception, keep_x_task_records) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
            //.get("SELECT id, job_id, upstream_ids, title, command_type, command_text, overtime, retry_limit, create_time, update_time FROM qross_jobs_dags WHERE job_id>1")
            //    .put("INSERT INTO qross_jobs_dags (id, job_id, upstream_ids, title, command_type, command_text, overtime, retry_limit, create_time, update_time) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
            //.get("SELECT id, job_id, dependency_moment, dependency_type, dependency_value, retry_limit, create_time, update_time FROM qross_jobs_dependencies WHERE job_id>1")
            //    .put("INSERT INTO qross_jobs_dependencies (id, job_id, dependency_moment, dependency_type, dependency_value, retry_limit, create_time, update_time) VALUES (?, ?, ?, ?, ?, ?, ?, ?)")
            //.get("SELECT id, job_id, task_time, status, start_time, finish_time, spent, checked, create_time, update_time FROM qross_tasks WHERE job_id>1")
            //    .put("INSERT INTO qross_tasks (id, job_id, task_time, status, start_time, finish_time, spent, checked, create_time, update_time) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
            //.get("SELECT id, job_id, task_id, upstream_ids, command_id, status, retry_times, start_time, run_time, finish_time, elapsed, waiting, create_time, update_time FROM qross_tasks_dags WHERE job_id>1")
            //    .put("INSERT INTO qross_tasks_dags (id, job_id, task_id, upstream_ids, command_id, status, retry_times, start_time, run_time, finish_time, elapsed, waiting, create_time, update_time) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
            //.get("SELECT id, job_id, task_id, dependency_id, dependency_moment, dependency_type, dependency_value, ready, retry_times, create_time, update_time FROM qross_tasks_dependencies WHERE job_id>1")
            //    .put("INSERT INTO qross_tasks_dependencies (id, job_id, task_id, dependency_id, dependency_moment, dependency_type, dependency_value, ready, retry_times, create_time, update_time) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
            //.get("SELECT id, job_id, task_id, command_id, action_id, log_type, log_text, create_time FROM qross_tasks_logs WHERE job_id>1")
            //    .put("INSERT INTO qross_tasks_logs (id, job_id, task_id, command_id, action_id, log_type, log_text, create_time) VALUES (?, ?, ?, ?, ?, ?, ?, ?)")

//        dh.open("mysql.qross")
//          .set("ALTER TABLE qross_jobs MODIFY COLUMN id INT AUTO_INCREMENT ")
//          .set("ALTER TABLE qross_jobs_dags MODIFY COLUMN id INT AUTO_INCREMENT")
//          .set("ALTER TABLE qross_jobs_dependencies MODIFY COLUMN id INT AUTO_INCREMENT")
//          .set("ALTER TABLE qross_tasks MODIFY COLUMN id BIGINT AUTO_INCREMENT")
//          .set("ALTER TABLE qross_tasks_dags MODIFY COLUMN id BIGINT AUTO_INCREMENT")
//          .set("ALTER TABLE qross_tasks_dependencies MODIFY COLUMN id BIGINT AUTO_INCREMENT")
//          .set("ALTER TABLE qross_tasks_logs MODIFY COLUMN id BIGINT AUTO_INCREMENT PRIMARY KEY")
//
//        dh.close()
    }
}
