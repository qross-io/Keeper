DEBUG OFF;

--需要参数 $metadata, $prefix

IF $metadata IS UNDEFINED THEN
    SET $metadata := 'mysql.qross';
END IF;

IF $prefix IS UNDEFINED THEN
    SET $prefix := 'qross_';
END IF;

OPEN $metadata;
SAVE TO $metadata;

VAR $connections := SELECT id, open_mode, database_type, connection_name, connection_string, jdbc_driver, username, password FROM qross_connections WHERE database_class='system' AND enabled='yes';

FOR $connection OF $connections LOOP
    CALL $SCRAPE($connection);
END LOOP;

FUNCTION $SCRAPE ($connection)
    BEGIN

        PRINT "Scrape Start: " + $connection.connection_name;

        --OPEN $connection.connection_name;
        CASE $connection.open_mode
            WHEN 'properties' THEN
                OPEN $connection.connection_name;
            WHEN 'nacos' THEN
                LOAD NACOS PROPERTIES $connection.connection_string;
                OPEN $connection.connection_name;
            ELSE
                OPEN DATABASE $connection.connection_string DRIVER $connection.jdbc_driver USERNAME $connection.username PASSWORD $connection.password AS $connection.connection_name;
        END CASE;

        CASE $connection.database_type
            WHEN 'PostgreSQL' THEN
                SET $not_in := "('information_schema', 'pg_temp_1', 'pg_toast_temp_1', 'public', 'pg_toast', 'pg_catalog')";
                CACHE 'SCHEMATA' # SELECT catalog_name AS database_name, schema_name FROM information_schema.SCHEMATA WHERE schema_name NOT IN $not_in!;
                CACHE 'TABLES' # SELECT c.catalog_name AS database_name, a.schemaname AS table_schema, a.tablename AS table_name, 0 as table_rows, d.description as table_comment, 0 auto_increment, null as create_time, null as update_time
                                    FROM (SELECT schemaname, tablename FROM pg_catalog.pg_tables WHERE schemaname not in $not_in!) a
                                    INNER JOIN pg_catalog.pg_class b ON a.tablename = b.relname
                                    INNER JOIN information_schema.SCHEMATA c ON a.schemaname=c.schema_name
                                    LEFT JOIN pg_catalog.pg_description d ON d.objoid = b."oid" AND d.objsubid = '0';
                CACHE 'COLUMNS' # SELECT g.catalog_name AS database_name, a.schemaname AS table_schema, a.tablename AS table_name, c.attname AS column_name, f.column_default AS column_default, e.typname AS column_type, d.description AS column_comment
                                    FROM (SELECT schemaname, tablename FROM pg_catalog.pg_tables WHERE schemaname NOT IN $not_in!) a
                                    INNER JOIN pg_catalog.pg_class b ON a.tablename = b.relname
                                    INNER JOIN pg_catalog.pg_attribute c ON c.attrelid = b."oid" and c.attnum > 0
                                    INNER JOIN pg_catalog.pg_description d ON d.objoid = c.attrelid AND d.objsubid = c.attnum
                                    INNER JOIN pg_catalog.pg_type e ON e."oid" = c.atttypid
                                    INNER JOIN information_schema.columns f ON f.table_name = b.relname AND f.column_name = c.attname
                                    INNER JOIN information_schema.schemata g ON a.schemaname=g.schema_name;
            WHEN 'Oracle' THEN
                SET $database_name := SELECT name FROM v$database;
                CACHE 'SCHEMATA' # SELECT $database_name AS database_name, username AS schema_name  FROM sys.dba_users WHERE account_status = 'OPEN' AND username NOT IN ('SYSTEM', 'SYS', 'ATOM', 'DBSNMP');
                CACHE 'TABLES' # SELECT $database_name AS database_name, a.username AS table_schema, b.table_name AS table_name, NVL(b.num_rows, 0) AS table_rows, c.comments AS table_comment, 0 AS auto_increment, d.created AS create_time, d.last_ddl_time AS update_time
                                        FROM sys.dba_users a
                                        LEFT JOIN sys.dba_tables b ON  a.username = b.owner
                                        INNER JOIN dba_tab_comments c ON c.table_name = b.table_name AND c.owner = b.owner
                                        INNER JOIN sys.dba_objects d ON d.object_name = b.table_name
                                        WHERE a.account_status = 'OPEN' AND a.username NOT IN ('SYSTEM', 'SYS', 'ATOM', 'DBSNMP');
                CACHE 'COLUMNS' # SELECT $database_name AS database_name, A.USERNAME AS table_schema, B.TABLE_NAME AS table_name, C.COLUMN_NAME AS column_name, D.DATA_DEFAULT AS column_default, D.DATA_TYPE AS column_type, C.COMMENTS AS column_comment
                                    FROM SYS.DBA_USERS A
                                    LEFT JOIN SYS.DBA_TABLES B ON A.USERNAME = B.OWNER
                                    INNER JOIN SYS.DBA_COL_COMMENTS C ON C.TABLE_NAME = B.TABLE_NAME AND C.OWNER = B.OWNER INNER JOIN SYS.DBA_TAB_COLUMNS D ON D.TABLE_NAME = C.TABLE_NAME AND D.OWNER = C.OWNER AND D.COLUMN_NAME = C.COLUMN_NAME
                                    WHERE A.ACCOUNT_STATUS = 'OPEN' AND A.USERNAME NOT IN ('SYSTEM', 'SYS', 'ATOM', 'DBSNMP');
            WHEN 'SQLServer' THEN
                CACHE 'SCHEMATA' # SELECT CATALOG_NAME AS database_name, schema_name AS schema_name FROM information_schema.SCHEMATA
                                        WHERE schema_name not in ('guest', 'INFORMATION_SCHEMA', 'sys', 'db_owner', 'db_accessadmin', 'db_securityadmin', 'db_ddladmin', 'db_backupoperator', 'db_datareader', 'db_datawriter', 'db_denydatareader', 'db_denydatawriter' );
                CACHE 'TABLES' # SELECT a.TABLE_CATALOG AS database_name, a.TABLE_SCHEMA AS table_schema,  a.TABLE_NAME AS table_name, 0 AS table_rows, c.value AS table_comment, 0 AS auto_increment, b.crdate as create_time, b.refdate as update_time
                        FROM information_schema.tables a
                         INNER JOIN sysobjects b ON  a.TABLE_NAME = b.name
                         LEFT JOIN sys.extended_properties c ON	b.id = c.major_id AND c.minor_id = 0
                         WHERE a.TABLE_NAME NOT IN ('spt_fallback_db', 'spt_fallback_dev', 'spt_fallback_usg', 'spt_values', 'spt_monitor', 'MSreplication_options');
                CACHE 'COLUMNS' # SELECT o.table_catalog AS database_name, o.table_schema AS table_schema, p.table_name, p.column_name, p.column_default, p.data_type AS column_type, p.column_description AS column_comment
                                    FROM information_schema.tables o
                                    INNER JOIN (
                                        SELECT a.name AS table_name, b.name AS column_name, c.value AS column_description, e.name AS data_type, f.definition AS column_default
                                        FROM sys.tables a
                                        INNER JOIN sys.columns b ON b.object_id = a.object_id
                                        LEFT JOIN sys.extended_properties c ON c.major_id = b.object_id AND c.minor_id = b.column_id
                                        INNER JOIN sys.types e ON b.system_type_id = e.system_type_id
                                        LEFT JOIN sys.default_constraints f ON a.object_id = f.parent_object_id AND b.column_id = f.parent_column_id) p
                                    ON o.table_name = p.table_name
	                                    AND o.table_name NOT IN ('spt_fallback_db', 'spt_fallback_dev', 'spt_fallback_usg', 'spt_values', 'spt_monitor', 'msreplication_options');
            WHEN 'Hive' THEN
                CACHE 'SCHEMATA' # SELECT CTLG_NAME AS database_name, NAME AS schema_name FROM DBS;
                CACHE 'TABLES' # SELECT F.CTLG_NAME AS database_name, F.NAME AS table_schema, A.TBL_NAME AS table_name, B.PARAM_VALUE AS table_rows, C.PARAM_VALUE AS table_comment, 0 AS auto_increment, null AS create_time, null AS update_time
                                    FROM TBLS A
                                    INNER JOIN DBS F ON A.DB_ID = F.DB_ID
                                    INNER JOIN ( SELECT TBL_ID, PARAM_VALUE FROM TABLE_PARAMS WHERE PARAM_KEY = 'numRows') B ON A.TBL_ID = B.TBL_ID
                                    LEFT JOIN (SELECT TBL_ID, PARAM_VALUE FROM TABLE_PARAMS WHERE PARAM_KEY = 'comment') C ON A.TBL_ID = C.TBL_ID;
                CACHE 'COLUMNS' # SELECT B.CTLG_NAME AS database_name, B.NAME AS table_schema, A.TBL_NAME AS table_name, C.COLUMN_NAME AS column_name, '' AS column_default, C.TYPE_NAME AS column_type, C.COMMENT AS column_comment
                                    FROM TBLS A
                                    INNER JOIN DBS B ON A.DB_ID = B.DB_ID
                                    INNER JOIN COLUMNS_V2 C ON A.TBL_ID = C.CD_ID;
            --Phoenix
	    WHEN 'Phoenix' THEN
                CACHE 'SCHEMATA' # SELECT null AS database_name,TABLE_SCHEM AS schema_name  FROM SYSTEM.CATALOG WHERE TABLE_SCHEM <> 'SYSTEM' GROUP BY TABLE_SCHEM;
                CACHE 'TABLES' # SELECT
                                NULL AS database_name,
                                a.TABLE_SCHEMA AS table_schema,
                                a.TABLE_NAME AS table_name,
                                b.TABLE_ROWS AS table_rows,
                                NULL AS table_comment,
                                NULL AS auto_increment,
                                NULL AS create_time,
                                NULL AS update_time
                                FROM
                                (
                                SELECT
                                    TABLE_SCHEM AS table_schema,
                                    TABLE_NAME AS table_name
                                from
                                    SYSTEM.CATALOG
                                where TABLE_SCHEM <> 'SYSTEM' AND COLUMN_NAME IS NOT NULL)a
                                LEFT JOIN
                                (select COUNT(1) AS TABLE_ROWS,
                                    TABLE_NAME,
                                    TABLE_SCHEM
                                FROM SYSTEM.CATALOG
                                WHERE TABLE_SCHEM <> 'SYSTEM' AND COLUMN_NAME IS NOT NULL
                                GROUP BY TABLE_NAME,TABLE_SCHEM
                                ) b ON a.table_schema=b.TABLE_SCHEM AND a.table_name=b.TABLE_NAME;
                CACHE 'COLUMNS' # SELECT null AS database_name, TABLE_SCHEM AS table_schema, TABLE_NAME AS table_name, COLUMN_NAME AS column_name, NULL AS column_default, null AS column_type, null AS column_comment from SYSTEM.CATALOG WHERE TABLE_SCHEM <> 'SYSTEM' AND COLUMN_NAME IS NOT NULL;
            ELSE
                --MySQL
                SET $not_in := "('information_schema', 'dbhealth', 'mysql', 'performance_schema', 'binlog_', 'test', 'sys', 'sakila')";
                CACHE 'SCHEMATA' # SELECT schema_name AS database_name, schema_name FROM information_schema.SCHEMATA WHERE schema_name NOT IN $not_in!;
                CACHE 'TABLES' # SELECT table_schema AS database_name, table_schema, table_name, table_rows, table_comment, auto_increment, DATE_FORMAT(create_time, '%Y-%m-%d %H:%i:%s') AS create_time, DATE_FORMAT(update_time, '%Y-%m-%d %H:%i:%s') AS update_time FROM information_schema.TABLES WHERE table_schema NOT IN $not_in! AND table_name<>'schema_version';
                CACHE 'COLUMNS' # SELECT table_schema AS database_name, table_schema, table_name, column_name, left(column_default,100) AS column_default, column_type, column_comment FROM information_schema.COLUMNS WHERE table_schema NOT IN $not_in! AND table_name<>'schema_version';
        END CASE;


        PRINT 'Scrape: ' + $connection.connection_name + ", CACHE OK";

        OPEN $metadata;
        CACHE 'current_databases' # SELECT id, database_name, schema_name, connection_id, enabled FROM $prefix!_metadata_databases WHERE connection_id=$connection.id;

        OPEN CACHE;
        -- record the max id
        SET $max := SELECT MAX(id) FROM current_databases -> FIRST CELL -> IF NULL 0;

        -- new databases
        GET # SELECT DISTINCT $connection.id, A.database_name, A.schema_name
                 FROM SCHEMATA A
                    LEFT JOIN current_databases B ON A.database_name=B.database_name AND A.schema_name=B.schema_name AND A.schema_name=B.schema_name WHERE B.database_name IS NULL;
        PUT # INSERT INTO $prefix!_metadata_databases (connection_id, database_name, schema_name) VALUES (?, ?, ?);
        --miss databases
        GET # SELECT A.id FROM current_databases A LEFT JOIN SCHEMATA B ON A.database_name=B.database_name AND A.schema_name=B.schema_name WHERE B.schema_name IS NULL;
        PUT # UPDATE $prefix!_metadata_databases SET missed='yes' WHERE id=?;
        -- update scrape time
        PREP # UPDATE $prefix!_metadata_databases SET scrape_time=NOW() WHERE connection_id=$connection.id;

        -- refresh databases in cache
        OPEN $metadata;
        CACHE 'current_databases' # SELECT id, database_name, schema_name, connection_id, enabled FROM $prefix!_metadata_databases WHERE connection_id=$connection.id AND id>$max;

        PRINT "Scrape: " + $connection.connection_name + ", SCRAPE DATABASES OK";

        --exists tables
        CACHE 'current_tables' # SELECT A.id, A.connection_id, A.database_id, B.database_name, B.schema_name, B.enabled, A.table_name, A.table_comment, A.table_rows,
                    A.create_time, A.update_time, A.auto_increment FROM $prefix!_metadata_tables A INNER JOIN $prefix!_metadata_databases B ON A.database_id=B.id
                     WHERE A.connection_id=$connection.id;

        -- different tables
        OPEN CACHE;
        -- record the max table id
        SET $max := SELECT MAX(id) FROM current_tables -> FIRST CELL -> IF NULL 0;
        CACHE 'diff_tables' # SELECT A.id AS table_id, A.table_comment AS current_comment,
                    B.table_comment AS update_comment, A.table_rows AS current_rows, B.table_rows AS update_rows, A.update_time AS current_time,
                    B.update_time AS update_time, A.auto_increment AS current_increment, B.auto_increment AS update_increment FROM current_tables A
                    INNER JOIN TABLES B ON A.database_name=B.database_name AND A.schema_name=B.table_schema AND A.table_name=B.table_name WHERE (A.table_comment<>B.table_comment
                    OR A.table_rows<>B.table_rows OR A.auto_increment<>B.auto_increment OR A.update_time<>B.update_time);

        OPEN CACHE;
        -- insert if not exists
        GET # SELECT D.connection_id, D.id, C.table_name, C.table_comment, C.table_rows, C.create_time, C.update_time,
               IFNULL(C.auto_increment, 0) AS auto_increment FROM
                (SELECT A.* FROM TABLES A LEFT JOIN current_tables B ON A.table_schema=B.schema_name AND A.database_name=B.database_name
                    AND A.table_name=B.table_name WHERE B.table_name IS NULL) C
                     INNER JOIN current_databases D ON C.table_schema=D.schema_name AND C.database_name=D.database_name  WHERE D.enabled='yes';
            PUT # INSERT INTO $prefix!_metadata_tables (connection_id, database_id, table_name, table_comment, table_rows, create_time, update_time, auto_increment) VALUES (?, ?, ?, ?, ?, ?, ?, ?);
            PREP # UPDATE $prefix!_metadata_tables SET scrape_time=NOW() WHERE connection_id=$connection.id;

        -- select missed
        GET # SELECT A.id FROM current_tables A LEFT JOIN TABLES B ON A.database_name=B.database_name AND A.schema_name=B.table_schema AND A.table_name=B.table_name WHERE B.table_name IS NULL;
            PUT # UPDATE $prefix!_metadata_tables SET missed='yes' WHERE id=?;
        -- update comment
        GET # SELECT update_comment, table_id FROM diff_tables WHERE current_comment<>update_comment;
            PUT # UPDATE $prefix!_metadata_tables SET table_comment=? WHERE id=?;
        -- update rows
        GET # SELECT update_rows, table_id FROM diff_tables WHERE current_rows<>update_rows;
            PUT # UPDATE $prefix!_metadata_tables SET table_rows=? WHERE id=?;
        -- update update_time
        GET # SELECT update_time, table_id FROM diff_tables WHERE current_time<>update_time;
            PUT # UPDATE $prefix!_metadata_tables SET update_time=? WHERE id=?;
        -- update max value of primary key
        GET # SELECT update_increment, table_id FROM diff_tables WHERE current_increment<>update_increment;
            PUT # UPDATE $prefix!_metadata_tables SET auto_increment=? WHERE id=?;

        OPEN $metadata;
        -- refresh tables
        CACHE 'current_tables' # SELECT A.id, A.connection_id, A.database_id, B.database_name, B.schema_name, B.enabled, A.table_name, A.table_comment, A.table_rows,
                    A.create_time, A.update_time, A.auto_increment FROM $prefix!_metadata_tables A INNER JOIN $prefix!_metadata_databases B ON A.database_id=B.id
                     WHERE A.connection_id=$connection.id AND A.id>$max;

        PRINT "Scrape: " + $connection.connection_name + ", SCRAPE TABLES OK";

        -- exists columns
        CACHE 'current_columns' # SELECT A.id, A.connection_id, A.database_id, C.database_name, C.schema_name, C.enabled, A.table_id, B.table_name,
                                A.column_name, A.column_default, A.column_type, A.column_comment
                                FROM $prefix!_metadata_columns A
                                INNER JOIN $prefix!_metadata_tables B ON A.table_id=B.id
                                INNER JOIN $prefix!_metadata_databases C ON A.database_id=C.id
                                WHERE A.connection_id=$connection.id;

        -- different columns
        OPEN CACHE;
        CACHE 'diff_columns' # SELECT A.id AS column_id,
                    A.column_type AS current_type, B.column_type AS update_type,
                    A.column_default AS current_default, B.column_default AS update_default,
                    A.column_comment AS current_comment, B.column_comment AS update_comment
                    FROM current_columns A
                    INNER JOIN COLUMNS B ON A.database_name=B.database_name AND A.schema_name=B.table_schema AND A.table_name=B.table_name AND A.column_name=B.column_name
                    WHERE (A.column_type<>B.column_type OR A.column_default<>B.column_default AND A.column_comment<>B.column_comment);

        -- insert if not exists
        OPEN CACHE;
        GET # SELECT D.connection_id, D.database_id, D.id,
                    C.column_name, C.column_type, C.column_default, C.column_comment
                    FROM (SELECT A.* FROM COLUMNS A LEFT JOIN current_columns B ON A.table_schema=B.schema_name AND A.database_name=B.database_name AND A.table_name=B.table_name AND A.column_name=B.column_name WHERE B.column_name IS NULL) C
                    INNER JOIN current_tables D ON C.table_schema=D.schema_name AND c.database_name=D.database_name AND C.table_name=D.table_name WHERE D.enabled='yes';
             PUT # INSERT INTO $prefix!_metadata_columns (connection_id, database_id, table_id, column_name, column_type, column_default, column_comment) VALUES (?, ?, ?, ?, ?, ?, ?);
             PREP # UPDATE $prefix!_metadata_columns SET scrape_time=NOW() WHERE connection_id=$connection.id;

        --missed columns;
        GET # SELECT A.id FROM current_columns A LEFT JOIN COLUMNS B ON A.database_name=B.database_name AND A.schema_name=B.table_schema AND A.table_name=B.table_name AND A.column_name=B.column_name WHERE B.column_name IS NULL;
            PUT # UPDATE $prefix!_metadata_columns SET missed='yes' WHERE id=?;
        -- details
        GET # SELECT column_id, 'column_type' AS update_item, update_type FROM diff_columns WHERE current_type<>update_type;
        GET # SELECT column_id, 'column_default' AS update_item, update_default FROM diff_columns WHERE current_default<>update_default;
        GET # SELECT column_id, 'column_comment' AS update_item, SUBSTR(update_comment, 1, 500) FROM diff_columns WHERE current_comment<>update_comment;
            PUT # INSERT INTO $prefix!_metadata_columns_details (column_id, update_item, update_value) VALUES (?, ?, ?);
        -- column_type
        GET # SELECT update_type, column_id FROM diff_columns WHERE current_type<>update_type;
            PUT # UPDATE $prefix!_metadata_columns SET column_type=? WHERE id=?;
        -- column_default
        GET # SELECT update_default, column_id FROM diff_columns WHERE current_default<>update_default;
            PUT # UPDATE $prefix!_metadata_columns SET column_default=? WHERE id=?;
        -- column_comment
        GET # SELECT update_comment, column_id FROM diff_columns WHERE current_comment<>update_comment;
            PUT # UPDATE $prefix!_metadata_columns SET column_comment=? WHERE id=?;

        PRINT "Scrape: " + $connection.connection_name + ", SCRAPE COLUMNS OK";

        DROP TABLE 'SCHEMATA';
        DROP TABLE 'TABLES';
        DROP TABLE 'COLUMNS';
        DROP TABLE 'current_databases';
        DROP TABLE 'current_tables';
        DROP TABLE 'diff_tables';
        DROP TABLE 'current_columns';
        DROP TABLE 'diff_columns';

        PRINT "Scrape End: " + $connection.connection_name;
    END;
