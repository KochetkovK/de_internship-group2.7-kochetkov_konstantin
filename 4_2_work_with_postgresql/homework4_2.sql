DROP TABLE IF EXISTS users;
DROP TABLE IF EXISTS users_audit; 

CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name TEXT,
    email TEXT,
    role TEXT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE users_audit (
    id SERIAL PRIMARY KEY,
    user_id INTEGER,
    changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    changed_by TEXT,
    field_changed TEXT,
    old_value TEXT,
    new_value TEXT
);

INSERT INTO users (name, email, role)
VALUES 
('Ivan Ivanov', 'ivan@example.com', 'administrator'),
('Anna Petrova', 'anna@example.com', 'user'),
('Sergey Sergeev', 'sergey@example.com', 'user'),
('Alexey Alexeev', 'alexey@example.com', 'owner');

CREATE OR REPLACE FUNCTION audit_user_update()
RETURNS TRIGGER AS $$
BEGIN
    IF OLD.name IS DISTINCT FROM NEW.name THEN
	    INSERT INTO users_audit (user_id, changed_by, field_changed, old_value, new_value)
	    VALUES (OLD.id, current_user, 'name', OLD.name, NEW.name);
    END IF;
    IF OLD.email IS DISTINCT FROM NEW.email THEN
	    INSERT INTO users_audit (user_id, changed_by, field_changed, old_value, new_value)
	    VALUES (OLD.id, current_user, 'email', OLD.email, NEW.email);
    END IF;
    IF OLD."role" IS DISTINCT FROM NEW."role" THEN
	    INSERT INTO users_audit (user_id, changed_by, field_changed, old_value, new_value)
	    VALUES (OLD.id, current_user, 'role', OLD."role", NEW."role");
    END IF;      
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_audit_user_update 
BEFORE UPDATE ON users
FOR EACH ROW 
EXECUTE FUNCTION audit_user_update();

UPDATE users
SET name = 'Petr Ivanov', email = 'petr@example.com', updated_at = CURRENT_TIMESTAMP
WHERE name = 'Ivan Ivanov';

UPDATE users
SET name = 'Anna Ivanova', updated_at = CURRENT_TIMESTAMP
WHERE name = 'Anna Petrova';

UPDATE users
SET email = 'anna_admin@example.com', "role" = 'administrator', updated_at = CURRENT_TIMESTAMP
WHERE name = 'Anna Ivanova';

SELECT * FROM users u;
SELECT * FROM users_audit ua;

CREATE OR REPLACE FUNCTION export_users_audit()
RETURNS void AS $func$
DECLARE
    export_path TEXT := CONCAT('/tmp/users_audit_export_', 
                               TO_CHAR(CURRENT_TIMESTAMP, 'YYYYMMDD_HH24MI'), 
                               '.csv');
BEGIN
    EXECUTE format(
        $$ 
			COPY 
			    (SELECT	id, user_id, changed_at, changed_by, field_changed, old_value, new_value
			     FROM users_audit
			     WHERE changed_at::DATE = CURRENT_DATE - INTERVAL '1 day')
			TO %L WITH CSV HEADER
        $$, export_path);    
END;
$func$ LANGUAGE plpgsql;

SELECT export_users_audit();


SELECT cron.schedule(
    'export_users_audit_daily',
    '0 3 * * *',
    $$SELECT export_users_audit();$$);


SELECT * FROM cron.job;
/*
jobid|schedule |command                     |nodename |nodeport|database  |username|active|jobname                 
-----+---------+----------------------------+---------+--------+----------+--------+------+------------------------
    1|0 3 * * *|SELECT export_users_audit();|localhost|    5432|example_db|user    |true  |export_users_audit_daily
*/

SELECT * FROM cron.job_run_details;

-- Вывод терминала

/* 
root@1e5de7cf93bf:/# cd tmp
root@1e5de7cf93bf:/tmp# ls
users_audit_export_20251030_0300.csv  users_audit_export_20251031_0300.csv
root@1e5de7cf93bf:/tmp# cat users*_20251031_0300.csv
id,user_id,changed_at,changed_by,field_changed,old_value,new_value
1,1,2025-10-30 22:26:02.666213,user,name,Ivan Ivanov,Petr Ivanov
2,1,2025-10-30 22:26:02.666213,user,email,ivan@example.com,petr@example.com
3,2,2025-10-30 22:26:07.65406,user,name,Anna Petrova,Anna Ivanova
4,2,2025-10-30 22:26:11.466482,user,email,anna@example.com,anna_admin@example.com
5,2,2025-10-30 22:26:11.466482,user,role,user,administrator
root@1e5de7cf93bf:/tmp# 
*/
















