
SET $metadata := 'mysql.datax';
SET $prefix := '';

OPEN $metadata;
SAVE TO $metadata;

GET # SELECT A.id AS connection_id, A.database_type, A.connection_name, A.connection_string, A.jdbc_driver, A.username, A.password, B.id AS rule_id, B.quality_rule FROM qross_connections A
    INNER JOIN $prefix!metadata_qualities B ON A.id=B.connection_id AND A.database_class='system' AND B.enabled='yes';

PAR # RUN PQL '/pql/metadata_quality_check.sql';



