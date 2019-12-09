PRINT 'Cleaning tasks mechanism is ready to execute.';

VAR $TO_CLEAR := SELECT B.job_id, A.keep_x_task_records FROM qross_jobs A
                    INNER JOIN (SELECT job_id, COUNT(0) AS task_amount FROM qross_tasks GROUP BY job_id) B ON A.id=B.job_id
                        WHERE A.keep_x_task_records>0 AND B.task_amount>A.keep_x_task_records;
FOR $job_id, $keep_tasks IN $TO_CLEAR
  LOOP
    SET $task_id := SELECT id AS task_id FROM qross_tasks WHERE job_id=$job_id ORDER BY id DESC LIMIT $keep_tasks,1;

    FOR $id, $create_time IN (SELECT id, create_time FROM qross_tasks WHERE job_id=$job_id AND id<=$task_id)
      LOOP
        DELETE FILE @QROSS_HOME + "tasks/" + $job_id + "/" + ${ $create_time REPLACE "-" TO "" SUBSTRING 1 TO 9 } + "/" + $id + ".log";
      END LOOP;

    DELETE FROM qross_tasks_logs WHERE job_id=$job_id AND task_id<=$task_id;
    DELETE FROM qross_tasks_dependencies WHERE job_id=$job_id AND task_id<=$task_id;
    DELETE FROM qross_tasks_dags WHERE job_id=$job_id AND task_id<=$task_id;
    DELETE FROM qross_tasks_events WHERE job_id=$job_id AND task_id<=$task_id;
    DELETE FROM qross_tasks_records WHERE job_id=$job_id AND task_id<=$task_id;
    SET $rows := DELETE FROM qross_tasks WHERE job_id=$job_id AND id<=$task_id;
    INSERT INTO qross_jobs_clean_records (job_id, amount, info) VALUES ($job_id, $rows, $task_id);

    PRINT DEBUG $rows + ' tasks of job ' + $job_id + ' has been deleted.';
  END LOOP;

PRINT 'Tasks cleaning has finished.';