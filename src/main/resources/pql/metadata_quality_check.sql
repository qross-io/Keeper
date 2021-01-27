
SAVE TO 'mysql.datax';
SET $prefix := '';

OPEN DATABASE $connection_string DRIVER $jdbc_driver USERNAME $username PASSWORD $password AS $connection_name;

PRINT $quality_rule;
GET # EXEC $quality_rule -> WHERE 'amount > 0';
PUT # REPLACE INTO $prefix!metadata_qualities_reports (rule_id, item, exceptional, report_date) VALUES ($rule_id, &item, #amount, ${ @today FORMAT 'yyyy-MM-dd'});
