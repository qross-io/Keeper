DEBUG ON;

SET $days := @KEEP_LOGS_FOR_X_DAYS;

IF $days > 0 THEN
    SET $point := @NOW MINUS DAYS $days FORMAT 'yyyy-MM-dd HH:mm:ss';

    DELETE FROM qross_stuck_tasks WHERE create_time<$point;
    DELETE FROM qross_keeper_exceptions WHERE create_time<$point;
    DELETE FROM qross_server_monitor WHERE moment<$point;

    SET $day := @NOW FORMAT 'yyyyMMdd';

    -- beats logs
    FOR $file OF (FILE LIST '''@QROSS_HOME/keeper/beats/''') LOOP
        IF $file.name TAKE BEFORE '.' < $day THEN
            FILE DELETE $file.path;
        END IF;
    END LOOP;

    -- Keeper logs
    FOR $dir OF (DIR LIST '''@QROSS_HOME/keeper/logs/''') LOOP
        IF $dir.name < $day THEN
            DIR DELETE $dir.path;
        END IF;
    END LOOP;
END IF;

PRINT DEBUG 'System records and logs have been cleaned.';