
-- 每小时整点统计上一小时的任务运行情况
-- 新建任务数量，运行的任务数量，运行的脚本数量

DEBUG ON;

OPEN QROSS;

PRINT INFO 'Keeper Stat begin.';

SET $date := '#{date}';
SET $hour := '#{hour}';
SET $current := '#{date} #{hour}:00:00'; -- 当前小时
SET $previous := $current MINUS 1 HOUR -> FORMAT 'yyyy-MM-dd HH:00:00'; -- 上一小时

PRINT 'DATE: ' + $date;
PRINT 'HOUR: ' + $hour;
PRINT 'CURRENT: ' + $current;
PRINT 'PREVIOUS: ' + $previous;

SET $new_tasks := SELECT COUNT(0) AS amount FROM qross_tasks WHERE create_time>=$previous AND create_time<$current;
-- start_time, finish_time
SET $executing_tasks := SELECT COUNT(0) AS amount FROM qross_tasks WHERE (finish_time>=$previous AND finish_time<=$current) OR (start_time>=$previous AND start_time<=$current);
SET $executing_records := SELECT COUNT(0) AS amount FROM qross_tasks_records WHERE (finish_time>=$previous AND finish_time<=$current) OR (start_time>=$previous AND start_time<=$current);
-- run_time, finish_time
SET $running_actions := SELECT COUNT(0) AS amount FROM qross_tasks_dags WHERE (finish_time>=$previous AND finish_time<=$current) OR (run_time>=$previous AND run_time<=$current);

INSERT INTO qross_tasks_hour_stat (stat_hour, new_tasks, executing_tasks, running_actions) VALUES ($current, $new_tasks, ${ $executing_tasks + $executing_records }, $running_actions);

PRINT 'Hour stat finished.';

-- 新建任务数，异常任务数
IF $hour == '00' THEN
    -- 0点统计全天
    SET $yesterday := $current MINUS 1 DAY -> FORMAT 'yyyy-MM-dd 00:00:00'; -- 昨天

    SET $new_tasks := SELECT COUNT(0) AS amount FROM qross_tasks WHERE create_time>=$yesterday AND create_time<$current;
    SET $exceptional_tasks := SELECT COUNT(0) AS amount FROM qross_tasks WHERE create_time>=$yesterday AND create_time<$current AND status IN ('failed', 'incorrect', 'checking_limit', 'timeout');
    SET $exceptional_records := SELECT COUNT(0) AS amount FROM qross_tasks_records WHERE create_time>=$yesterday AND create_time<$current AND status IN ('failed', 'incorrect', 'checking_limit', 'timeout');

    INSERT INTO qross_tasks_day_stat (stat_date, new_tasks, exceptional_tasks) VALUES ($date, $new_tasks, ${ $exceptional_tasks + $exceptional_records });

    PRINT 'Day stat finished.';
END IF;

PRINT INFO 'Keeper Stat end.';