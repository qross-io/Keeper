DEBUG OFF;

SET $metadata_database := 'mysql.qross';
IF $scrape IS UNDEFINED THEN
    SET $scrape := 'all';
END IF;

OPEN $metadata_database;
SAVE TO $metadata_database;

IF $connection_name IS UNDEFINED THEN
    VAR $connections := SELECT id, database_type, connection_name, connection_string, jdbc_driver, username, password, default_database FROM
                qross_connections WHERE enabled='yes' AND metadata_connection_name='' AND connection_name NOT IN
                (SELECT DISTINCT metadata_connection_name FROM qross_connections WHERE metadata_connection_name<>'')
                    UNION ALL
            SELECT A.id, A.database_type, B.connection_name, B.connection_string, B.jdbc_driver, B.username, B.password,B.default_database  FROM qross_connections A
                INNER JOIN qross_connections B ON A.metadata_connection_name=B.connection_name WHERE A.enabled='yes' AND A.metadata_connection_name <> '';

    FOR $connection OF $connections LOOP
        CALL $SCRAPE($connection);
    END LOOP;
ELSE
    VAR $connection := SELECT A.id, A.database_type, A.connection_name,
                                IF(B.connection_string IS NULL, A.connection_string, B.connection_string) AS connection_string,
                                IF(B.jdbc_driver IS NULL, A.jdbc_driver, B.jdbc_driver) AS jdbc_driver,
                                IF(B.username IS NULL, A.username, B.username) AS username,
                                IF(B.password IS NULL, A.password, B.password) AS password,
                                IF(A.scrape_time IS NULL, 120, TIMESTAMPDIFF(SECOND, A.scrape_time, NOW())) AS timespan FROM qross_connections A
                        LEFT JOIN qross_connections B ON A.metadata_connection_name=B.connection_name WHERE A.connection_name=$connection_name AND A.enabled='yes';

    IF $connection IS NOT EMPTY THEN
        IF $connection.first.timespan >= 120 THEN
            CALL $SCRAPE($connection.first);
        END IF;
    ELSE
        PRINT ERROR 'Too Incorrect connection name: #{connection_name}';
    END IF;
END IF;

FUNCTION $SCRAPE ($connection)
    BEGIN

        PRINT "Scrape Start: " + $connection.connection_name;

        PRINT $connection;
        PRINT;

        --OPEN $connection.connection_name;
        OPEN DATABASE $connection.connection_string DRIVER $connection.jdbc_driver USERNAME $connection.username PASSWORD $connection.password AS $connection.connection_name;

        PRINT $connection.database_type;
        PRINT $connection.connection_name;

        CASE $connection.database_type
            WHEN 'PostgreSQL' THEN
                SET $not_in := "('information_schema', 'pg_temp_1', 'pg_toast_temp_1', 'public', 'pg_toast', 'pg_catalog')";
                CACHE 'SCHEMATA' # SELECT catalog_name AS database_name, schema_name FROM information_schema.SCHEMATA WHERE schema_name NOT IN $not_in!;
                CACHE 'TABLES' # SELECT c.catalog_name AS database_name, a.schemaname AS table_schema, a.tablename AS table_name, 0 as table_rows, d.description as table_comment, 0 auto_increment, null as create_time, null as update_time
                                    FROM (SELECT schemaname, tablename FROM pg_catalog.pg_tables WHERE schemaname not in $not_in!) a
                                    LEFT JOIN pg_catalog.pg_class b ON a.tablename = b.relname
                                    LEFT JOIN information_schema.SCHEMATA c ON a.schemaname=c.schema_name
                                    LEFT JOIN pg_catalog.pg_description d ON d.objoid = b."oid" AND d.objsubid = '0';
                CACHE 'COLUMNS' # select a_1.database_name, a_1.table_schema, a_1.table_name, a_1.column_name, a_1.column_default, a_1.column_type, a_1.column_comment,h.isnull,
                                    (case when h.pk_name is null then 'false' else 'true' end) as constraint_type,
                                    (case when i.index_name is null then 'false' else 'true' end ) as index_name
                                    from
                                    (
                                    SELECT g.catalog_name AS database_name, a.schemaname AS table_schema, a.tablename AS table_name, c.attname AS column_name, f.column_default AS column_default, e.typname AS column_type, d.description AS column_comment
                                                                        FROM (SELECT schemaname, tablename FROM pg_catalog.pg_tables WHERE schemaname NOT IN $not_in!) a
                                                                        LEFT JOIN pg_catalog.pg_class b ON a.tablename = b.relname
                                                                        LEFT JOIN pg_catalog.pg_attribute c ON c.attrelid = b."oid" and c.attnum > 0
                                                                        LEFT JOIN pg_catalog.pg_description d ON d.objoid = c.attrelid AND d.objsubid = c.attnum
                                                                        LEFT JOIN pg_catalog.pg_type e ON e."oid" = c.atttypid
                                                                        LEFT JOIN information_schema.columns f ON f.table_name = b.relname AND f.column_name = c.attname
                                                                        LEFT JOIN information_schema.schemata g ON a.schemaname=g.schema_name)a_1
                                                                        left join
                                                                        (select pg_class.relname,
                                                                        pg_attribute.attname as colname,
                                                                        pg_constraint.conname as pk_name,
                                                                        pg_attribute.attnotnull as isnull
                                                                        from pg_catalog.pg_constraint inner join pg_catalog.pg_class on pg_constraint.conrelid = pg_class.oid
                                                                        inner join pg_catalog.pg_attribute on pg_attribute.attrelid = pg_class.oid
                                                                        and pg_attribute.attnum = pg_constraint.conkey[1]
                                                                        where  pg_constraint.contype='p'
                                                                        group by pg_class.relname,pg_attribute.attname,pg_constraint.conname,pg_attribute.attnotnull
                                                                        ) h on a_1.table_name=h.relname and a_1.column_name=h.colname
                                                                        left join
                                                                        (
                                                                        select
                                                                            t.relname as table_name,
                                                                            i.relname as index_name,
                                                                            a.attname as column_name
                                                                        from
                                                                            pg_class t,
                                                                            pg_class i,
                                                                            pg_index ix,
                                                                            pg_attribute a
                                                                        where
                                                                            t.oid = ix.indrelid
                                                                            and i.oid = ix.indexrelid
                                                                            and a.attrelid = t.oid
                                                                            and a.attnum = ANY(ix.indkey)
                                                                            and t.relkind = 'r'
                                                                        group by t.relname,i.relname,a.attname
                                                                        ) i on a_1.table_name=i.table_name and a_1.column_name=i.column_name;

            WHEN 'Oracle' THEN
                SET $database_name := SELECT name FROM v~u0024database;
                CACHE 'SCHEMATA' # SELECT $database_name AS DATABASE_NAME, USERNAME AS SCHEMA_NAME FROM USER_USERS WHERE ACCOUNT_STATUS = 'OPEN';
                CACHE 'TABLES' # SELECT $database_name AS DATABASE_NAME,(SELECT USERNAME FROM USER_USERS WHERE ACCOUNT_STATUS = 'OPEN') AS TABLE_SCHEMA,A.TABLE_NAME,A.TABLE_ROWS,B.COMMENTS AS TABLE_COMMENT,0 AS AUTO_INCREMENT,C.CREATED AS CREATE_TIME,C.LAST_DDL_TIME AS UPDATE_TIME 
                                    FROM (SELECT TABLE_NAME,NVL(NUM_ROWS,0) AS TABLE_ROWS FROM USER_TABLES)A
                                    LEFT JOIN																(SELECT TABLE_NAME,COMMENTS FROM USER_TAB_COMMENTS WHERE TABLE_TYPE = 'TABLE')B
                                    ON A.TABLE_NAME=B.TABLE_NAME						    							    LEFT JOIN(SELECT OBJECT_NAME,CREATED,LAST_DDL_TIME FROM USER_OBJECTS WHERE OBJECT_TYPE='TABLE')C
                                    ON A.TABLE_NAME=C.OBJECT_NAME;
                CACHE 'COLUMNS' #  SELECT 
									$database_name AS DATABASE_NAME,
									(SELECT USERNAME FROM USER_USERS WHERE ACCOUNT_STATUS = 'OPEN') AS TABLE_SCHEMA, 
									A.TABLE_NAME,A.COLUMN_NAME,B.DATA_DEFAULT AS COLUMN_DEFAULT,
									B.DATA_TYPE AS COLUMN_TYPE,A.COMMENTS AS COLUMN_COMMENT,
									B.nullable AS isnull,
									(CASE WHEN C.constraint_type ='P' THEN 'true' ELSE 'false' END ) constraint_type,
									C.index_name
									FROM 
									(SELECT TABLE_NAME,COLUMN_NAME,COMMENTS FROM USER_COL_COMMENTS)A 
									LEFT JOIN 
									(SELECT TABLE_NAME,COLUMN_NAME,nullable,DATA_DEFAULT,DATA_TYPE FROM USER_TAB_COLUMNS)B 
									ON A.TABLE_NAME=B.TABLE_NAME AND A.COLUMN_NAME=B.COLUMN_NAME
									LEFT JOIN (select ucc.table_name, ucc.column_name,uc.constraint_type,uc.index_name from user_cons_columns ucc, user_constraints uc where uc.constraint_name = ucc.constraint_name and uc.constraint_type = 'P')C ON A.COLUMN_NAME=C.COLUMN_NAME AND A.TABLE_NAME=C.table_name;

            WHEN 'SQL Server' THEN
                CACHE 'SCHEMATA' # SELECT CATALOG_NAME AS database_name, schema_name AS schema_name FROM information_schema.SCHEMATA
                                        WHERE schema_name not in ('guest', 'INFORMATION_SCHEMA', 'sys', 'db_owner', 'db_accessadmin', 'db_securityadmin', 'db_ddladmin', 'db_backupoperator', 'db_datareader', 'db_datawriter', 'db_denydatareader', 'db_denydatawriter' );
                CACHE 'TABLES' # SELECT a.TABLE_CATALOG AS database_name, a.TABLE_SCHEMA AS table_schema,  a.TABLE_NAME AS table_name, 0 AS table_rows, c.value AS table_comment, 0 AS auto_increment, b.crdate as create_time, b.refdate as update_time
                        FROM information_schema.tables a
                         INNER JOIN sysobjects b ON  a.TABLE_NAME = b.name
                         LEFT JOIN sys.extended_properties c ON	b.id = c.major_id AND c.minor_id = 0
                         WHERE a.TABLE_NAME NOT IN ('spt_fallback_db', 'spt_fallback_dev', 'spt_fallback_usg', 'spt_values', 'spt_monitor', 'MSreplication_options');
                CACHE 'COLUMNS' # SELECT o.table_catalog AS database_name, o.table_schema AS table_schema, p.table_name, p.column_name, p.column_default, p.data_type AS column_type, p.column_description AS column_comment,(case when p.is_nullable='0' then 'true' else 'false' end) AS isnull, (case when e.constraint_columns is null then 'false' else 'true' end) AS constraint_type,(case when f.constraint_columns is null then 'false' else 'true' end) AS index_name
                                    FROM information_schema.tables o
                                    INNER JOIN (
                                        SELECT a.name AS table_name, b.name AS column_name, c.value AS column_description, e.name AS data_type, f.definition AS column_default,b.is_nullable
                                        FROM sys.tables a
                                        INNER JOIN sys.columns b ON b.object_id = a.object_id
                                        LEFT JOIN sys.extended_properties c ON c.major_id = b.object_id AND c.minor_id = b.column_id
                                        INNER JOIN sys.types e ON b.system_type_id = e.user_type_id
                                        LEFT JOIN sys.default_constraints f ON a.object_id = f.parent_object_id AND b.column_id = f.parent_column_id) p
                                    ON o.table_name = p.table_name
	                                AND o.table_name NOT IN ('spt_fallback_db', 'spt_fallback_dev', 'spt_fallback_usg', 'spt_values', 'spt_monitor', 'msreplication_options')
	                               LEFT JOIN (SELECT
											tab.name AS table_name,
											idx.name AS constrain_name,
											idx.type_desc as constraint_type,
											col.name AS constraint_columns
											FROM
											sys.indexes idx
											JOIN sys.index_columns idxCol
											ON (idx.object_id = idxCol.object_id
											AND idx.index_id = idxCol.index_id
											AND idx.is_primary_key = 1)
											JOIN sys.tables tab
											ON (idx.object_id = tab.object_id)
											JOIN sys.columns col
											ON (idx.object_id = col.object_id
											AND idxCol.column_id = col.column_id))e ON o.table_name=e.table_name AND p.column_name=e.constraint_columns
											LEFT JOIN (
											SELECT
										tab.name AS table_name,
										idx.is_unique  as is_unique,
										idx.name AS constrain_name,
										idx.type_desc as constraint_type,
										col.name AS constraint_columns
										FROM
										sys.indexes idx
										JOIN sys.index_columns idxCol 
										ON (idx.object_id = idxCol.object_id 
										AND idx.index_id = idxCol.index_id 
										AND idx.is_unique_constraint= 0 and is_primary_key=0)
										JOIN sys.tables tab
										ON (idx.object_id = tab.object_id)
										JOIN sys.columns col
										ON (idx.object_id = col.object_id
										AND idxCol.column_id = col.column_id))f ON o.table_name=f.table_name AND p.column_name=f.constraint_columns;
            WHEN 'Hive' THEN
                CACHE 'SCHEMATA' # SELECT NAME AS database_name, NAME AS schema_name FROM hive.DBS;
                CACHE 'TABLES' # SELECT F.NAME AS database_name, F.NAME AS table_schema, A.TBL_NAME AS table_name, B.PARAM_VALUE AS table_rows, C.PARAM_VALUE AS table_comment, 0 AS auto_increment, null AS create_time, null AS update_time
                                    FROM hive.TBLS A
                                    LEFT JOIN hive.DBS F ON A.DB_ID = F.DB_ID
                                    LEFT JOIN ( SELECT TBL_ID, PARAM_VALUE FROM hive.TABLE_PARAMS WHERE PARAM_KEY = 'numRows') B ON A.TBL_ID = B.TBL_ID
                                    LEFT JOIN (SELECT TBL_ID, PARAM_VALUE FROM hive.TABLE_PARAMS WHERE PARAM_KEY = 'comment') C ON A.TBL_ID = C.TBL_ID;
                CACHE 'COLUMNS' # SELECT B.NAME AS database_name, B.NAME AS table_schema, A.TBL_NAME AS table_name, C.COLUMN_NAME AS column_name, '' AS column_default, C.TYPE_NAME AS column_type, C.COMMENT AS column_comment,
                                    '' AS isnull,'' AS constraint_type,'' AS index_name
                                                    FROM hive.TBLS A
                                                    LEFT JOIN hive.DBS B ON A.DB_ID = B.DB_ID
                                                    LEFT JOIN hive.COLUMNS_V2 C ON A.TBL_ID = C.CD_ID;
            --Phoenix
	    WHEN 'Phoenix' THEN
                CACHE 'SCHEMATA' # SELECT TABLE_SCHEM AS database_name,TABLE_SCHEM AS schema_name  FROM SYSTEM.CATALOG WHERE TABLE_SCHEM <> 'SYSTEM' GROUP BY TABLE_SCHEM;
                CACHE 'TABLES' # SELECT
                                a.TABLE_SCHEMA AS database_name,
                                a.TABLE_SCHEMA AS table_schema,
                                a.TABLE_NAME AS table_name,
                                b.TABLE_ROWS AS table_rows,
                                '' AS table_comment,
                                '' AS auto_increment,
                                '' AS create_time,
                                '' AS update_time
                                FROM
                                (
                                SELECT
                                    TABLE_SCHEM AS table_schema,
                                    TABLE_NAME AS table_name
                                from
                                    SYSTEM.CATALOG
                                where TABLE_SCHEM <> 'SYSTEM' AND COLUMN_NAME IS NOT NULL GROUP BY TABLE_NAME,TABLE_SCHEM)a
                                LEFT JOIN
                                (select COUNT(1) AS TABLE_ROWS,
                                    TABLE_NAME,
                                    TABLE_SCHEM
                                FROM SYSTEM.CATALOG
                                WHERE TABLE_SCHEM <> 'SYSTEM' AND COLUMN_NAME IS NOT NULL
                                GROUP BY TABLE_NAME,TABLE_SCHEM
                                ) b ON a.table_schema=b.TABLE_SCHEM AND a.table_name=b.TABLE_NAME;
                CACHE 'COLUMNS' # SELECT TABLE_SCHEM AS database_name, 
                                    TABLE_SCHEM AS table_schema,
                                    TABLE_NAME AS table_name,
                                    COLUMN_NAME AS column_name,
                                    COLUMN_DEF AS column_default,
                                    (CASE DATA_TYPE WHEN 4 THEN 'INT'
                                    WHEN -5 THEN 'BIGINT'
                                    WHEN -6 THEN 'TINYINT'
                                    WHEN 5 THEN 'SMALLINT'
                                    WHEN 6 THEN 'FLOAT'
                                    WHEN 8 THEN 'DOUBLE'
                                    WHEN 4 THEN 'UNSIGNED_INT'
                                    WHEN -5 THEN 'UNSIGNED_LONG'
                                    WHEN -6 THEN 'UNSIGNED_TINYINT'
                                    WHEN 3 THEN 'DECIMAL'
                                    WHEN 16 THEN 'BOOLEAN'
                                    WHEN 92 THEN 'TIME'
                                    WHEN 91 THEN 'DATE'
                                    WHEN 93 THEN 'TIMESTAMP'
                                    WHEN 12 THEN 'VARCHAR'
                                    WHEN 1 THEN 'CHAR'
                                    WHEN -2 THEN 'BINARY'
                                    WHEN -3 THEN 'VARBINARY' ELSE '0' END) AS column_type,
                                    '' AS column_comment,
                                    '' AS isnull,
                                    '' AS constraint_type,
                                    '' AS index_name
                                from SYSTEM.CATALOG
                                WHERE TABLE_SCHEM <> 'SYSTEM' AND COLUMN_NAME IS NOT NULL;
            ELSE
                --MySQL
                SET $not_in := "('information_schema', 'dbhealth', 'mysql', 'performance_schema', 'binlog_','sys', 'sakila','qross','dolphinscheduler')";
                CACHE 'SCHEMATA' # SELECT schema_name AS database_name, schema_name FROM information_schema.SCHEMATA WHERE schema_name NOT IN $not_in!;
                CACHE 'TABLES' # SELECT table_schema AS database_name, table_schema, table_name, table_rows, table_comment, auto_increment, DATE_FORMAT(create_time, '%Y-%m-%d %H:%i:%s') AS create_time, DATE_FORMAT(update_time, '%Y-%m-%d %H:%i:%s') AS update_time FROM information_schema.TABLES WHERE table_schema NOT IN $not_in! AND table_name<>'schema_version';
                CACHE 'COLUMNS' # select
                                    a.database_name,
                                    a.table_schema,
                                    a.table_name,
                                    a.column_name,
                                    a.column_default,
                                    a.column_type,
                                    a.column_comment,
                                    a.is_nullable AS isnull,
                                    (case when c.constraint_name is null then 'false' else 'true' end) as constraint_type,
                                    (case when b.index_name is null then 'false' else 'true' end) as index_name
                                    from
                                    (
                                    SELECT
                                    table_schema AS database_name,
                                    table_schema,
                                    table_name,
                                    column_name,
                                    left(column_default,100) AS column_default,
                                    data_type AS column_type,
                                    column_comment,
                                    is_nullable
                                    FROM information_schema.COLUMNS
                                    WHERE table_schema NOT IN $not_in! AND table_name<>'schema_version'
                                    )a
                                    left join
                                    (
                                    SELECT table_schema,table_name,index_name,COLUMN_NAME
                                    FROM information_schema.statistics
                                    where index_name not in('PRIMARY') and table_schema not in $not_in!
                                    ) b on a.table_schema=b.table_schema and a.table_name =b.table_name and a.column_name=b.column_name
                                    left join
                                    (
                                    select table_schema,table_name,column_name,constraint_name
                                    from information_schema.key_column_usage
                                    where table_schema not in $not_in!
                                    ) c on a.table_schema=c.table_schema and a.table_name=c.table_name and a.column_name=c.column_name;
        END CASE;

        OPEN CACHE;

        DEBUG ON;
        SELECT * FROM SCHEMATA;
        SELECT * FROM TABLES;
        SELECT * FROM COLUMNS;
        DEBUG OFF;

        PRINT 'Scrape: ' + $connection.connection_name + ", CACHE OK";

        OPEN $metadata_database;
        CACHE 'current_databases' # SELECT id, database_name, schema_name, connection_id, enabled FROM qross_metadata_databases WHERE connection_id=$connection.id;

        OPEN CACHE;
        -- record the max id
        SET $max := SELECT MAX(id) FROM current_databases -> FIRST CELL -> IF NULL 0;

        -- new databases
        GET # SELECT DISTINCT $connection.id, A.database_name, A.schema_name
                 FROM SCHEMATA A
                    LEFT JOIN current_databases B ON A.database_name=B.database_name AND A.schema_name=B.schema_name AND A.schema_name=B.schema_name WHERE B.database_name IS NULL;
        PUT # INSERT INTO qross_metadata_databases (connection_id, database_name, schema_name) VALUES (?, ?, ?);
        --miss databases
        GET # SELECT A.id FROM current_databases A LEFT JOIN SCHEMATA B ON A.database_name=B.database_name AND A.schema_name=B.schema_name WHERE B.schema_name IS NULL;
        PUT # UPDATE qross_metadata_databases SET missed='yes' WHERE id=?;
        -- non missed
        GET # SELECT A.id FROM current_databases A LEFT JOIN SCHEMATA B ON A.database_name=B.database_name AND A.schema_name=B.schema_name WHERE B.schema_name IS NOT NULL;
        PUT # UPDATE qross_metadata_databases SET missed='no' WHERE id=? AND missed='yes';
        -- update scrape time
        PREP # UPDATE qross_metadata_databases SET scrape_time=NOW() WHERE connection_id=$connection.id;

        -- refresh databases in cache
        OPEN $metadata_database;
        CACHE 'current_databases' # SELECT id, database_name, schema_name, connection_id, enabled FROM qross_metadata_databases WHERE connection_id=$connection.id AND id>$max;

        PRINT "Scrape: " + $connection.connection_name + ", SCRAPE DATABASES OK";

        --exists tables
        CACHE 'current_tables' # SELECT A.id, A.connection_id, A.database_id, B.database_name, B.schema_name, B.enabled, A.table_name, A.table_comment, A.table_rows,
                    A.create_time, A.update_time, A.auto_increment FROM qross_metadata_tables A INNER JOIN qross_metadata_databases B ON A.database_id=B.id
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
            PUT # INSERT INTO qross_metadata_tables (connection_id, database_id, table_name, table_comment, table_rows, create_time, update_time, auto_increment) VALUES (?, ?, ?, ?, ?, ?, ?, ?);
            PREP # UPDATE qross_metadata_tables SET scrape_time=NOW() WHERE connection_id=$connection.id;

        -- select missed
        GET # SELECT A.id FROM current_tables A LEFT JOIN TABLES B ON A.database_name=B.database_name AND A.schema_name=B.table_schema AND A.table_name=B.table_name WHERE B.table_name IS NULL;
            PUT # UPDATE qross_metadata_tables SET missed='yes' WHERE id=?;
        -- non missed
        GET # SELECT A.id FROM current_tables A LEFT JOIN TABLES B ON A.database_name=B.database_name AND A.schema_name=B.table_schema AND A.table_name=B.table_name WHERE B.table_name IS NOT NULL;
            PUT # UPDATE qross_metadata_tables SET missed='no' WHERE id=? AND missed='yes';
        -- update comment
        GET # SELECT update_comment, table_id FROM diff_tables WHERE current_comment<>update_comment;
            PUT # UPDATE qross_metadata_tables SET table_comment=? WHERE id=?;
        -- update rows
        GET # SELECT update_rows, table_id FROM diff_tables WHERE current_rows<>update_rows;
            PUT # UPDATE qross_metadata_tables SET table_rows=? WHERE id=?;
        -- update update_time
        GET # SELECT update_time, table_id FROM diff_tables WHERE current_time<>update_time;
            PUT # UPDATE qross_metadata_tables SET update_time=? WHERE id=?;
        -- update max value of primary key
        GET # SELECT update_increment, table_id FROM diff_tables WHERE current_increment<>update_increment;
            PUT # UPDATE qross_metadata_tables SET auto_increment=? WHERE id=?;

        OPEN $metadata_database;
        -- refresh tables
        CACHE 'current_tables' # SELECT A.id, A.connection_id, A.database_id, B.database_name, B.schema_name, B.enabled, A.table_name, A.table_comment, A.table_rows,
                    A.create_time, A.update_time, A.auto_increment FROM qross_metadata_tables A INNER JOIN qross_metadata_databases B ON A.database_id=B.id
                     WHERE A.connection_id=$connection.id AND A.id>$max;

        PRINT "Scrape: " + $connection.connection_name + ", SCRAPE TABLES OK";

        IF $scrape != 'table' THEN
            -- exists columns
            CACHE 'current_columns' # SELECT A.id, A.connection_id, A.database_id, C.database_name, C.schema_name, C.enabled, A.table_id, B.table_name,
                                    A.column_name, A.column_default, A.column_type, A.column_comment, A.isnull, A.constraint_type, A.index_name
                                    FROM qross_metadata_columns A
                                    INNER JOIN qross_metadata_tables B ON A.table_id=B.id
                                    INNER JOIN qross_metadata_databases C ON A.database_id=C.id
                                    WHERE A.connection_id=$connection.id;

            -- different columns
            OPEN CACHE;
            CACHE 'diff_columns' # SELECT A.id AS column_id,
                        A.column_type AS current_type, B.column_type AS update_type,
                        A.column_default AS current_default, B.column_default AS update_default,
                        A.column_comment AS current_comment, B.column_comment AS update_comment,
                        A.isnull AS current_isnull, B.isnull AS update_isnull,
                        A.constraint_type AS current_constraint, B.constraint_type AS update_constraint,
                        A.index_name AS current_index, B.index_name AS update_index
                        FROM current_columns A
                        INNER JOIN COLUMNS B ON A.database_name=B.database_name AND A.schema_name=B.table_schema AND A.table_name=B.table_name AND A.column_name=B.column_name
                        WHERE
                            (A.column_type<>B.column_type OR
                             A.column_default<>B.column_default OR
                             A.column_comment<>B.column_comment OR
                             A.isnull<>B.isnull OR
                             A.constraint_type<>B.constraint_type OR
                             A.index_name<>B.index_name);

            -- insert if not exists
            OPEN CACHE;
            GET # SELECT D.connection_id, D.database_id, D.id,
                        C.column_name, C.column_type, C.column_default, C.column_comment
                        FROM (SELECT A.* FROM COLUMNS A LEFT JOIN current_columns B ON A.table_schema=B.schema_name AND A.database_name=B.database_name AND A.table_name=B.table_name AND A.column_name=B.column_name WHERE B.column_name IS NULL) C
                        INNER JOIN current_tables D ON C.table_schema=D.schema_name AND c.database_name=D.database_name AND C.table_name=D.table_name WHERE D.enabled='yes';
                 PUT # INSERT INTO qross_metadata_columns (connection_id, database_id, table_id, column_name, column_type, column_default, column_comment) VALUES (?, ?, ?, ?, ?, ?, ?);
                 PREP # UPDATE qross_metadata_columns SET scrape_time=NOW() WHERE connection_id=$connection.id;

            --missed columns;
            GET # SELECT A.id FROM current_columns A LEFT JOIN COLUMNS B ON A.database_name=B.database_name AND A.schema_name=B.table_schema AND A.table_name=B.table_name AND A.column_name=B.column_name WHERE B.column_name IS NULL;
                PUT # UPDATE qross_metadata_columns SET missed='yes' WHERE id=?;
            GET # SELECT A.id FROM current_columns A LEFT JOIN COLUMNS B ON A.database_name=B.database_name AND A.schema_name=B.table_schema AND A.table_name=B.table_name AND A.column_name=B.column_name WHERE B.column_name IS NOT NULL;
                PUT # UPDATE qross_metadata_columns SET missed='no' WHERE id=? AND missed='yes';
            -- details
            GET # SELECT column_id, 'column_type' AS update_item, update_type FROM diff_columns WHERE current_type<>update_type;
            GET # SELECT column_id, 'column_default' AS update_item, update_default FROM diff_columns WHERE current_default<>update_default;
            GET # SELECT column_id, 'column_comment' AS update_item, SUBSTR(update_comment, 1, 500) FROM diff_columns WHERE current_comment<>update_comment;
            GET # SELECT column_id, 'isnull' AS update_itme, update_isnull FROM diff_columns WHERE current_isnull<>update_isnull;
            GET # SELECT column_id, 'constraint_type' AS update_itme, update_isnull FROM diff_columns WHERE current_constraint<>update_constraint;
            GET # SELECT column_id, 'index_name' AS update_itme, update_isnull FROM diff_columns WHERE current_index<>update_index;
                PUT # INSERT INTO qross_metadata_columns_details (column_id, update_item, update_value) VALUES (?, ?, ?);
            -- column_type
            GET # SELECT update_type, column_id FROM diff_columns WHERE current_type<>update_type;
                PUT # UPDATE qross_metadata_columns SET column_type=? WHERE id=?;
            -- column_default
            GET # SELECT update_default, column_id FROM diff_columns WHERE current_default<>update_default;
                PUT # UPDATE qross_metadata_columns SET column_default=? WHERE id=?;
            -- column_comment
            GET # SELECT update_comment, column_id FROM diff_columns WHERE current_comment<>update_comment;
                PUT # UPDATE qross_metadata_columns SET column_comment=? WHERE id=?;
            -- isnull
            GET # SELECT update_isnull, column_id FROM diff_columns WHERE current_isnull<>update_isnull;
                PUT # UPDATE qross_metadata_columns SET isnull=? WHERE id=?;
            -- constraint_type
            GET # SELECT update_constraint, column_id FROM diff_columns WHERE current_constraint<>update_constraint;
                PUT # UPDATE qross_metadata_columns SET constraint_type=? WHERE id=?;
            -- index_name
            GET # SELECT update_index, column_id FROM diff_columns WHERE current_index<>update_index;
                PUT # UPDATE qross_metadata_columns SET index_name=? WHERE id=?;

            PRINT "Scrape: " + $connection.connection_name + ", SCRAPE COLUMNS OK";
        END IF;

        DROP TABLE IF EXISTS 'SCHEMATA';
        DROP TABLE IF EXISTS 'TABLES';
        DROP TABLE IF EXISTS 'COLUMNS';
        DROP TABLE IF EXISTS 'current_databases';
        DROP TABLE IF EXISTS 'current_tables';
        DROP TABLE IF EXISTS 'diff_tables';
        DROP TABLE IF EXISTS 'current_columns';
        DROP TABLE IF EXISTS 'diff_columns';

        OPEN $metadata_database;
        UPDATE qross_connections SET scrape_time=NOW() WHERE id=$connection.id;

        PRINT "Scrape End: " + $connection.connection_name;
    END;
