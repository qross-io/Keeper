-- 参数列表
-- 连接 connection_name
-- 库类型 database_type
-- 库 database_name 可能会不传
-- 表 table_name
-- 字段 column_name
-- 字段类型 column_type 是否数字列
-- 比较规则 operator  等于 = / 不等于 != / 大于 > / 大于等于 >= /小于 < / 小于等于 <= / 在字典 IN / 不在字典内 NOT IN /  / 区间 ><
-- 比较值 comparison_value    NULL/EMPTY/0
-- 分区字段名 partition_name
-- 分组表达式 partition_expression 例如 yyyyMMdd
-- 自定义条件 condition
-- 自定义检查语句 sentence

PRINT '''检查开始''';
PRINT;

SET $quote := CASE ${ $database_type REPLACE ' ' TO '' TO LOWER }
				WHEN 'mysql' THEN '`'
				WHEN 'sqlserver' THEN '"'
				WHEN 'postgresql' THEN '"'
				WHEN 'oracle' THEN '"'
				WHEN 'hive' THEN '`'
				WHEN 'phoenix' THEN '"'
				ELSE '"' END;

IF $database_name IS UNDEFINED THEN
	SET $database_name := '';
END IF;
IF $database_name <> '' THEN
	-- 指定数据库名前缀
	SET $database_name := '''$(quote)#{database_name}$(quote).''';
	PRINT '''数据库名: $database_name''';
END IF;

SET $amount := 0;

-- 配置错误数据的规则
OPEN #{connection_name};
	IF $sentence IS UNDEFINED OR $sentence IS EMPTY THEN
		PRINT '标准检查';
		SET $where := '';
		IF $column_name IS NOT UNDEFINED AND $column_name != '' THEN
			IF $comparison_value == 'NULL' THEN
				IF $operator == '=' THEN
					SET $where := '''$(quote)#{column_name}$$(quote) IS NULL''';
				ELSE
					SET $where := '''$(quote)#{column_name}$(quote) IS NOT NULL''';
				END IF;
			ELSIF $operator IN ('>', '>=', '<', '<=') THEN
				IF $column_type == 'NUMBER' THEN
					SET $where := '''$(quote)#{column_name}$(quote) #{operator} #{comparison_value}''';
				ELSE
					SET $where := """$(quote)#{column_name}$(quote) #{operator} '#{comparison_value}'""";
				END IF;
			ELSIF $operator IN ('^=', '$=', '*=') THEN
				SET $where := """$(quote)#{column_name}$(quote) LIKE '${ CASE $operator WHEN '^=' THEN '#{comparison_value}%' WHEN '$=' THEN '%#{comparison_value}' ELSE '%#{comparison_value}%' END }'""";
			ELSIF $operator == 'NOT IN' THEN
				IF $column_type == 'NUMBER' THEN
					SET $where := '''$(quote)#{column_name}$(quote) NOT IN (${ $comparison_value REPLACE '，' TO ',' })''';
				ELSE
					SET $where := """$(quote)#{column_name}$(quote) NOT IN ('${ $comparison_value REPLACE '，' TO ',' REPLACE ',' TO "','" }')""";
				END IF;
			ELSIF $operator == 'IN' THEN
				IF $column_type == 'NUMBER' THEN
					SET $where := '''$(quote)#{column_name}$(quote) IN (${ $comparison_value REPLACE '，' TO ',' })''';
				ELSE
					SET $where := """$(quote)#{column_name}$(quote) IN ('${ $comparison_value REPLACE '，' TO ',' REPLACE ',' TO "','" }')""";
				END IF;
			-- range
			ELSIF $operator == '><' THEN
				IF $column_type == 'NUMBER' THEN
					SET $where := '''($(quote)#{column_name}$(quote) #{greater_operator} #{greater_value} AND $(quote)#{column_name}$(quote) #{less_operator} #{less_value})''';
				ELSE
					SET $where := """($(quote)#{column_name}$(quote) #{greater_operator} '#{greater_value}' AND $(quote)#{column_name}$(quote) #{less_operator} '#{less_value})'""";
				END IF;
			ELSIF $comparison_value == 'EMPTY' THEN
				SET $where := """$(quote)#{column_name}$(quote) #{operator} ''""";
			ELSE
				IF $column_type == 'NUMBER' THEN
					SET $where := '''$(quote)#{column_name}$(quote) #{operator} #{comparison_value}''';
				ELSE
					SET $where := """$(quote)#{column_name}$(quote) #{operator} '#{comparison_value}'""";
				END IF;
			END IF;
		END IF;

		-- 设置了附加查询条件
		IF $condition != '' THEN
			IF $where != '' THEN
				SET $where := '''(#{condition}) AND $where''';
			ELSE
				SET $where := '''(#{condition})''';
			END IF;
		END IF;

		-- 存在分区
		IF $partition_name != '' THEN
			IF $where != '' THEN
				SET $where := '''$(quote)#{partition_name}$(quote) ='${ $task_time FORMAT $partition_expression }' AND $where''';
			ELSE
				SET $wehre := '''$(quote)#{partition_name}$(quote) ='${ $task_time FORMAT $partition_expression }'''';
			END IF;
		END IF;
		PRINT '''检查语句: SELECT COUNT(0) AS amount FROM $(database_name)$(quote)#{table_name}$(quote) WHERE $where''';

		IF $where != '' THEN
			SET $amount := SELECT COUNT(0) AS amount FROM $database_name!$quote!#{table_name}$quote! WHERE $where!;
		ELSE
			SET $amount := SELECT COUNT(0) AS amount FROM $database_name!$quote!#{table_name}$quote!;
		END IF;
	ELSE
		PRINT '自定义检查';
		IF $sentence MATCHES "(?i)^\\s*SELECT\b" THEN
			SET $amount := #{sentence};
		ELSE
			SET $amount := -1;
		END IF;
	END IF;

PRINT '检查完成';
PRINT '''检查结果：$amount''';

IF $amount IS NULL THEN
	PRINT ERROR '检查结果应该是一个整数，不应为 NULL，请检查配置是否正确。';
	SET $amount := -1;
	EXIT CODE 1;
END IF;

SET $now := @NOW FORMAT 'yyyy-MM-dd HH:mm:ss';

PRINT '保存结果报告';
OPEN DEFAULT;
INSERT INTO qross_qualities_reports (job_id, task_id, command_id, item, amount, report_date, create_time) VALUES ($job_id, $task_id, $command_id, '#{column_name}', $amount, ${ $task_time FORMAT 'yyyy-MM-dd' }, $now);

PRINT '对比检查结果是否符合预期';
-- 检查结果是否符合预期
OPEN DEFAULT;
SET $report_id := SELECT id FROM qross_qualities_reports WHERE command_id=$command_id AND create_time=$now LIMIT 1; -- 取已经检查完成的结果
SET $rule_id, $threshold_type, $threshold_value := SELECT id, threshold_type, threshold_value FROM qross_qualities_rules WHERE schedule_id=(SELECT id FROM qross_schedules WHERE job_id=$job_id);

PRINT '''阈值类型：$threshold_type''';
PRINT '''对比值：$threshold_value''';

SET $status := 'normal';
SET $is_percent := '%';

CASE $threshold_type
	WHEN 'greater' THEN
		PRINT '大于规则';
		SET $is_percent := '';
		IF $amount > $threshold_value THEN
			SET $status := 'abnormal';
		END IF;
	WHEN 'equals' THEN
		PRINT '等于规则';
		SET $is_percent := '';
		IF $amount == $threshold_value THEN
			SET $status := 'abnormal';
		END IF;
	WHEN 'day' THEN
		PRINT '按天对比';
		SET $previous_amount := SELECT amount FROM qross_qualities_reports WHERE command_id=$command_id AND id<$report_id ORDER BY id DESC LIMIT 1;
		SET $fluctuation_value := Math.abs(($amount - $previous_amount) / $previous_amount) * 100;
		IF $previous_amount IS NOT NULL AND $previous_amount > 0 AND $fluctuation_value > $threshold_value THEN
			SET $status := 'abnormal';
		END IF;
	WHEN 'week' THEN
		PRINT '按周对比';
		SET $avg_amount := SELECT AVG(amount) FROM (SELECT amount FROM qross_qualities_reports WHERE command_id=$command_id AND id<$report_id ORDER BY id DESC LIMIT 7) A;
		SET $fluctuation_value := Math.abs(($amount - $avg_amount) / $avg_amount) * 100;
		PRINT '''上周平均：$avg_amount''';
		PRINT '''波动值：$fluctuation_value''';
		IF $avg_amount IS NOT NULL AND $avg_amount > 0 AND $fluctuation_value > $threshold_value THEN
			SET $status := 'abnormal';
		END IF;
	WHEN 'two-week' THEN
		PRINT '双周对比';
		SET $avg_amount := SELECT AVG(amount) FROM (SELECT amount FROM qross_qualities_reports WHERE command_id=$command_id AND id<$report_id ORDER BY id DESC LIMIT 14) A;
		SET $fluctuation_value := Math.abs(($amount - $avg_amount) / $avg_amount) * 100;
		IF $avg_amount IS NOT NULL AND $avg_amount > 0 AND $fluctuation_value > $threshold_value THEN
			SET $status := 'abnormal';
		END IF;
	WHEN 'month' THEN
		PRINT '按月对比';
		SET $avg_amount := SELECT AVG(amount) FROM (SELECT amount FROM qross_qualities_reports WHERE command_id=$command_id AND id<$report_id ORDER BY id DESC LIMIT 30) A;
		SET $fluctuation_value := Math.abs(($amount - $avg_amount) / $avg_amount) * 100;
		IF $avg_amount IS NOT NULL AND $avg_amount > 0 AND $fluctuation_value > $threshold_value THEN
			SET $status := 'abnormal';
		END IF;
END CASE;

UPDATE qross_qualities_reports SET rule_id=$rule_id, report_result=$status WHERE id=$report_id;

PRINT '''检查结果: $status''';

IF $status == 'abnormal' THEN
	PRINT '发送预警';
	SET $owners := SELECT owners FROM qross_qualities_rules WHERE command_id=$command_id;
	PRINT '''负责人ID: $owners''';
	SET $title := SELECT title FROM qross_jobs WHERE id=$job_id;
	IF $owners != '' THEN
		-- 邮件
		SET $recipients := SELECT CONCAT('<', `fullname`, '>', `email`) AS recipient FROM qross_users WHERE id IN ($owners!) AND email IS NOT NULL AND email<>'' -> FIRST COLUMN -> JOIN '; ';
		IF $recipients <> '' AND $recipients CONTAINS '@' THEN
			PRINT '''发送邮件到 $recipients''';
			SEND MAIL '数据质量检查预警 ———— ' + $title
				CONTENT '''数据质量检查任务：$title <br/> 发现不满足数据质量的问题，请及时处理。'''
				TO $recipients;
			PRINT '''发送邮件完成''';
		END IF;
		-- 钉钉
		SET $mobiles := SELECT GROUP_CONCAT(mobile) AS mobiles FROM qross_users WHERE id IN ($owners!) AND mobile IS NOT NULL AND mobile <> '';
		IF $mobiles != '' THEN
			PRINT '''钉钉消息接收人：$mobiles''';
			PRINT '''发送钉钉群消息''';
			REQUEST JSON API '''https://oapi.dingtalk.com/robot/send?access_token=@DINGTALK_DATAGO_TOKEN''' METHOD 'POST'
			DATA {
				"at": {
					"atMobiles": [ $mobiles! ],
					"isAtAll": false
				},
				"text": {
					"content": ${ "预警消息: 数据质量检查任务<" + $title + ">超过阈值（" + $threshold_value + $is_percent  + "）限定，请及时处理。" }
				},
				"msgtype": "text"
			};
			PRINT ${{ PARSE "/" }};
			PRINT '''发送钉钉群消息完成''';
		END IF;
	END IF;
	PRINT '''发送预警完成''';
END IF;

PRINT '检查完成';