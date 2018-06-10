
CREATE TABLE IF NOT EXISTS qross_conf (
    id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    conf_key VARCHAR(100),
    conf_value VARCHAR(1000)
);

INSERT INTO qross_conf (conf_key, conf_value) VALUES
    ('QROSS_VERSION', '0.5.3'),
    ('QROSS_HOME', 'C:/io.Qross/Keeper/build/libs/'),
    ('QUIT_ON_NEXT_BEAT', 'no'),
    ('JAVA_BIN_HOME', ''),
    ('KEEP_X_TASK_RECORDS', '1000'),
    ('EMAIL_NOTIFICATION', 'no'),
    ('EMAIL_SMTP_HOST', 'smtp.domain.com'),
    ('EMAIL_SMTP_PORT', '25'),
    ('EMAIL_SENDER_PERSONAL', 'KeeperAdmin'),
    ('EMAIL_SENDER_ACCOUNT', 'user@domain.com'),
    ('EMAIL_SENDER_PASSWORD', 'password'),
    ('EMAIL_SSL_AUTH_ENABLED', 'no'),
    ('EMAIL_MASTER_ACCOUNT', 'none'),
    ('KERBEROS_AUTH', 'no'),
    ('KRB_USER_PRINCIPAL', 'username'),
    ('KRB_KEYTAB_PATH', '/home/username/username.keytab'),
    ('KRB_KRB5CONF_PATH', '/etc/krb5.conf');

CREATE UNIQUE INDEX ix_qross_conf_key ON qross_conf (conf_key);

CREATE TABLE IF NOT EXISTS qross_users (
    id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    username VARCHAR(100),
    role VARCHAR(100) DEFAULT 'worker' COMMENT 'master/keeper/worker',
    email VARCHAR(100),
    password VARCHAR(1000),
    create_time DATETIME DEFAULT NOW(),
    last_login_time DATETIME
);

CREATE INDEX ix_qross_users_role ON qross_users (role);

CREATE TABLE IF NOT EXISTS qross_connections (
    id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    connection_name VARCHAR(100),
    database_type VARCHAR(100) DEFAULT 'mysql' COMMENT 'sqlite/hive/sqlserver, etc.',
    connection_string TEXT,
    username VARCHAR(100) DEFAULT '',
    password VARCHAR(255) DEFAULT '',
    as_default VARCHAR(100) DEFAULT 'no' COMMENT 'yes/no'
);

CREATE INDEX ix_qross_connections_connection_name ON qross_connections (connection_name);

CREATE TABLE IF NOT EXISTS qross_message_box (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    sender VARCHAR(100),
    message_type VARCHAR(100),
    message_key VARCHAR(100),
    message_text VARCHAR(1000),
    create_time DATETIME DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS qross_keeper_start_records (
    id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    method VARCHAR(100) DEFAULT 'manual' COMMENT 'manual/crontab',
    start_time DATETIME DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS qross_keeper_beats (
    id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    actor_name VARCHAR(100),
    status VARCHAR(100) DEFAULT 'rest' COMMENT 'running/rest',
    start_time DATETIME,
    quit_time DATETIME,
    last_beat_time DATETIME
);

INSERT INTO qross_keeper_beats (actor_name) VALUES ('Keeper'), ('Messager'), ('TaskProducer'), ('TaskStarter'), ('TaskChecker'), ('TaskExecutor');
CREATE UNIQUE INDEX ix_qross_keeper_beats_actor_name ON qross_keeper_beats (actor_name);

CREATE TABLE IF NOT EXISTS qross_jobs (
    id INT NOT NULL AUTO_INCREMENT PRIMARY KEY COMMENT 'job id',
    title VARCHAR(1000),
    job_type VARCHAR(100) DEFAULT 'user' COMMENT 'user/system',
    owner TEXT COMMENT 'name<email address>, e.g. FullName<username@domain.com>',
    description TEXT,
    enabled VARCHAR(100) DEFAULT 'yes' COMMENT 'true/yes/no',
    cron_exp VARCHAR(1000) DEFAULT '' COMMENT 'simple cron expression, does not support letter C, empty means endless tasks',
    next_tick VARCHAR(1000) DEFAULT '' COMMENT 'next tick to create task, empty means not setup yet or can be empty, format is yyyyMMddHHmmss',
    dependencies VARCHAR(100) DEFAULT 'no' COMMENT 'yes/no, has dependencies or not',
    mail_notification VARCHAR(100) DEFAULT 'yes',
    mail_master_on_exception VARCHAR(100) DEFAULT 'no',
    complement_missed_tasks VARCHAR(100) DEFAULT 'no' COMMENT 'yes/no, complement missed tasks on system starts up',
    checking_retry_limit INT DEFAULT 100 COMMENT '0 means infinite',
    executing_retry_limit INT DEFAULT 0 COMMENT 'retry times on failure',
    concurrent_limit INT DEFAULT 3 COMMENT 'max quantity of concurrent',
    create_time DATETIME DEFAULT NOW(),
    update_time DATETIME DEFAULT NOW()
);

ALTER TABLE qross_jobs ADD INDEX ix_qross_jobs_enabled (enabled);
ALTER TABLE qross_jobs ADD INDEX ix_qross_jobs_next_tick (next_tick);

INSERT INTO qross_jobs (title, job_type, enabled, cron_exp, mail_notification) VALUES ('Task Cleaner', 'system', 'no', '0 10 0/6 * * ? *', 'no');
INSERT INTO qross_jobs (title, job_type, enabled, cron_exp, mail_notification) VALUES ('Beats Mail Sender', 'system', 'no', '0 0 3,9,12,15,18 * * ? *', 'no');

CREATE TABLE IF NOT EXISTS qross_jobs_dependencies (
    id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    job_id INT,
    dependency_moment VARCHAR(100) DEFAULT 'before' COMMENT 'before/after',
    dependency_type VARCHAR(100) COMMENT 'task/hdfs/sql/api/file',
    dependency_value TEXT COMMENT 'json format, see detail in code',
    create_time DATETIME DEFAULT NOW(),
    update_time DATETIME DEFAULT NOW()
);
ALTER TABLE qross_jobs_dependencies ADD INDEX ix_qross_jobs_dependencies_job_id (job_id);

CREATE TABLE IF NOT EXISTS qross_jobs_dags (
    id INT NOT NULL AUTO_INCREMENT PRIMARY KEY COMMENT 'command id',
    job_id INT,
    upstream_ids VARCHAR(1000) DEFAULT '' COMMENT 'dependency command ids, e.g. (1)(3)...',
    title VARCHAR(1000) DEFAULT 'Default',
    command_type VARCHAR(100) DEFAULT 'shell' COMMENT 'shell or sql',
    command_text MEDIUMTEXT COMMENT 'program or code to execute',
    overtime INT DEFAULT 0 COMMENT 'seconds',
    create_time DATETIME DEFAULT NOW(),
    update_time DATETIME DEFAULT NOW()
);
ALTER TABLE qross_jobs_dags ADD INDEX ix_qross_jobs_dags_job_id (job_id);

INSERT INTO qross_jobs_dags (job_id, title, command_text) VALUES (157, 'Clean', '%JAVA_BIN_HOMEjava -cp %QROSS_HOMEqross-keeper-%QROSS_VERSION.jar io.qross.keeper.Cleaner');
INSERT INTO qross_jobs_dags (job_id, title, command_text) VALUES (158, 'Notify', '%JAVA_BIN_HOMEjava -cp %QROSS_HOMEqross-keeper-%QROSS_VERSION.jar io.qross.keeper.Notifier');
UPDATE qross_jobs SET enabled='true' WHERE id=157;
UPDATE qross_jobs SET enabled='true' WHERE id=158;

CREATE TABLE IF NOT EXISTS qross_tasks (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY COMMENT 'task id',
    job_id INT,
    task_time VARCHAR(100) COMMENT 'task time, e.g. yyyyMM,yyyyMMdd,yyyyMMddHH,yyyyMMddHHmm',
    status VARCHAR(100) DEFAULT 'new' COMMENT 'new/initialized=checking/miss_command/checking_limit/ready/executing/finished/incorrect/failed/timeout',
    retry_times INT DEFAULT 0,
    start_time DATETIME COMMENT 'start time of computing',
    finish_time DATETIME COMMENT 'finished time of computing',
    spent INT COMMENT 'seconds',
    checked VARCHAR(100) DEFAULT '' COMMENT 'will be yes/no on exception',
    create_time DATETIME DEFAULT NOW(),
    update_time DATETIME DEFAULT NOW()
);

ALTER TABLE qross_tasks ADD INDEX ix_qross_tasks_job_id (job_id);
ALTER TABLE qross_tasks ADD INDEX ix_qross_tasks_status (status);

CREATE TABLE IF NOT EXISTS qross_tasks_dependencies (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    job_id INT,
    task_id BIGINT,
    dependency_moment VARCHAR(100) DEFAULT 'before' COMMENT 'before/after',
    dependency_type VARCHAR(100),
    dependency_value TEXT,
    ready VARCHAR(100) DEFAULT 'no' COMMENT 'yes/no',
    retry_times INT DEFAULT 0,
    create_time DATETIME DEFAULT NOW(),
    update_time DATETIME DEFAULT NOW()
);

ALTER TABLE qross_tasks_dependencies ADD INDEX ix_qross_tasks_dependencies_job_id (job_id);
ALTER TABLE qross_tasks_dependencies ADD INDEX ix_qross_tasks_dependencies_task_id (task_id);

CREATE TABLE IF NOT EXISTS qross_tasks_dags (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY COMMENT 'action id',
    job_id INT,
    task_id BIGINT,
    upstream_ids VARCHAR(1000) DEFAULT '' COMMENT 'format (x)(y)(z), current upstreamids, will update after every command finish',
    command_id INT,
    status VARCHAR(100) DEFAULT 'waiting' COMMENT 'waiting/running/exceptional/timeout/done',
    create_time DATETIME DEFAULT NOW(),
    update_time DATETIME DEFAULT NOW()
);

CREATE INDEX ix_qross_tasks_dags_job_id ON qross_tasks_dags (job_id);
CREATE INDEX ix_qross_tasks_dags_task_id ON qross_tasks_dags (task_id);
CREATE INDEX ix_qross_tasks_dags_command_id ON qross_tasks_dags (command_id);
CREATE INDEX ix_qross_tasks_dags_status ON qross_tasks_dags (status);

CREATE TABLE IF NOT EXISTS qross_tasks_logs (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    job_id INT,
    task_id BIGINT,
    command_id INT,
    action_id BIGINT,
    log_type VARCHAR(100) DEFAULT 'INFO' COMMENT 'INFO,ERROR',
    log_text TEXT,
    create_time DATETIME DEFAULT NOW()
);

CREATE INDEX ix_qross_tasks_logs_task_id ON qross_tasks_logs (task_id);
CREATE INDEX ix_qross_tasks_logs_action_id ON qross_tasks_logs (action_id);
