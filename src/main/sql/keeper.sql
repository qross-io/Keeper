
DROP TABLE IF EXISTS qross_conf;
CREATE TABLE IF NOT EXISTS qross_conf (
    id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    conf_key VARCHAR(100),
    conf_value VARCHAR(1000)
);

INSERT INTO qross_conf (conf_key, conf_value) VALUES
    ('QROSS_VERSION', '0.5'),
    ('QUIT_ON_NEXT_BEAT', 'no'),
    ('KEEP_X_TASK_RECORDS', '1000'),
    ('CLEAN_TASK_RECORDS_FREQUENCY', '0 10 0/6 * * ? *'),
    ('EMAIL_NOTIFICATION', 'no'),
    ('BEATS_MAILING_FREQUENCY', '0 0 3,9,12,15,18 * * ? *'),
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

DROP TABLE IF EXISTS qross_users;
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

INSERT INTO qross_users (username)

DROP TABLE IF EXISTS qross_message_box;
CREATE TABLE IF NOT EXISTS qross_message_box (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    sender VARCHAR(100),
    message_type VARCHAR(100),
    message_key VARCHAR(100),
    message_text VARCHAR(1000),
    create_time DATETIME DEFAULT NOW()
);

DROP TABLE IF EXISTS qross_keeper_beats;
CREATE TABLE IF NOT EXISTS qross_keeper_beats (
    id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    actor_name VARCHAR(100),
    status VARCHAR(100) DEFAULT 'rest' COMMENT 'running/rest',
    start_time DATETIME,
    quit_time DATETIME,
    last_beat_time DATETIME
);

INSERT INTO qross_keeper_beats (actor_name) VALUES ('Keeper'), ('Messager'), ('GlobalController'), ('TaskProducer'), ('TaskStarter'), ('TaskChecker'), ('TaskExecutor');
CREATE UNIQUE INDEX ix_qross_keeper_beats_actor_name ON qross_keeper_beats (actor_name);

DROP TABLE IF EXISTS qross_jobs;
CREATE TABLE IF NOT EXISTS qross_jobs (
    id INT NOT NULL AUTO_INCREMENT PRIMARY KEY COMMENT 'job id',
    title VARCHAR(1000),
    owner TEXT COMMENT 'name<email address>, e.g. FullName<username@domain.com>',
    description TEXT,
    enabled VARCHAR(100) DEFAULT 'yes' COMMENT 'true/yes/no',
    cron_exp VARCHAR(1000) DEFAULT '' COMMENT 'simple cron expression, does not support letter C, empty means endless tasks',
    next_tick VARCHAR(1000) DEFAULT '' COMMENT 'next tick to create task, empty means not setup yet or can be empty, format is yyyyMMddHHmmss',
    dependencies VARCHAR(100) DEFAULT 'no' COMMENT 'yes/no, has dependencies or not',
    mail_notification VARCHAR(100) DEFAULT 'yes',
    complement_missed_tasks VARCHAR(100) DEFAULT 'no' COMMENT 'yes/no, complement missed tasks on system starts up',
    checking_retry_limit INT DEFAULT 100 COMMENT '0 means infinite',
    executing_retry_limit INT DEFAULT 0 COMMENT 'retry times on failure',
    concurrent_limit INT DEFAULT 3 COMMENT 'max quantity of concurrent',
    create_time DATETIME DEFAULT NOW(),
    update_time DATETIME DEFAULT NOW()
);

ALTER TABLE qross_jobs ADD INDEX ix_qross_jobs_enabled (enabled);
ALTER TABLE qross_jobs ADD INDEX ix_qross_jobs_next_tick (next_tick);

DROP TABLE IF EXISTS qross_jobs_dependencies;
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

DROP TABLE IF EXISTS qross_jobs_dags;
CREATE TABLE IF NOT EXISTS qross_jobs_dags (
    id INT NOT NULL AUTO_INCREMENT PRIMARY KEY COMMENT 'command id',
    job_id INT,
    upstream_ids VARCHAR(1000) DEFAULT '' COMMENT 'dependency command ids, e.g. (1)(3)...',
    title VARCHAR(1000),
    command_type VARCHAR(100) DEFAULT 'shell' COMMENT 'shell or sql',
    command_text MEDIUMTEXT COMMENT 'program or code to execute',
    create_time DATETIME DEFAULT NOW(),
    update_time DATETIME DEFAULT NOW()
);

ALTER TABLE qross_jobs_dags ADD INDEX ix_qross_jobs_dags_job_id (job_id);

DROP TABLE IF EXISTS qross_tasks;
CREATE TABLE IF NOT EXISTS qross_tasks (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY COMMENT 'task id',
    job_id INT,
    task_time VARCHAR(100) COMMENT 'task time, e.g. yyyyMM,yyyyMMdd,yyyyMMddHH,yyyyMMddHHmm',
    status VARCHAR(100) DEFAULT 'new' COMMENT 'new/initialized=checking/miss_command/checking_limit/ready/executing/finished/incorrect/failed',
    retry_times INT DEFAULT 0,
    start_time DATETIME COMMENT 'start time of computing',
    finish_time DATETIME COMMENT 'finished time of computing',
    spent INT COMMENT 'seconds',
    create_time DATETIME DEFAULT NOW(),
    update_time DATETIME DEFAULT NOW()
);

ALTER TABLE qross_tasks ADD INDEX ix_qross_tasks_job_id (job_id);
ALTER TABLE qross_tasks ADD INDEX ix_qross_tasks_status (status);


DROP TABLE IF EXISTS qross_tasks_dependencies;
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

DROP TABLE IF EXISTS qross_tasks_dags;
CREATE TABLE IF NOT EXISTS qross_tasks_dags (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY COMMENT 'action id',
    job_id INT,
    task_id BIGINT,
    upstream_ids VARCHAR(1000) DEFAULT '' COMMENT 'format (x)(y)(z), current upstreamids, will update after every command finish',
    command_id INT,
    status VARCHAR(100) DEFAULT 'waiting' COMMENT 'waiting/running/exceptional/done',
    create_time DATETIME DEFAULT NOW(),
    update_time DATETIME DEFAULT NOW()
);

CREATE INDEX ix_qross_tasks_dags_job_id ON qross_tasks_dags (job_id);
CREATE INDEX ix_qross_tasks_dags_task_id ON qross_tasks_dags (task_id);
CREATE INDEX ix_qross_tasks_dags_command_id ON qross_tasks_dags (command_id);
CREATE INDEX ix_qross_tasks_dags_status ON qross_tasks_dags (status);

DROP TABLE IF EXISTS qross_tasks_logs;
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

