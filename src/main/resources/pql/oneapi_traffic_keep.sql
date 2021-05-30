
-- 每天一次

OPEN QROSS;

FOR $id, $keep_details, $keep_summary IN (SELECT id, traffic_keep_details, traffic_keep_summary FROM qross_api_services) LOOP
    IF $keep_details > 0 THEN
        SET $last_details_day := @NOW MINUS $keep_details DAYS;
        DELETE FROM qross_api_traffic_details WHERE request_time<${ $last_details_day FORMAT 'yyyy-MM-dd HH:mm:ss' };
        DELETE FROM qross_api_traffic_summary WHERE stat_data<${ $last_details_day FORMAT 'yyyy-MM-dd' }
    END IF;

    IF $keep_summary > 0 THEN
        SET $last_summary_day := @NOW MINUS $keep_summary DAYS;
        DELETE FROM qross_api_traffic_queries_details WHERE request_time<${ $last_summary_day FORMAT 'yyyy-MM-dd HH:mm:ss' };
        DELETE FROM qross_api_traffic_queries_summary WHERE stat_data<${ $last_summary_day FORMAT 'yyyy-MM-dd' }
    END IF;
END LOOP;