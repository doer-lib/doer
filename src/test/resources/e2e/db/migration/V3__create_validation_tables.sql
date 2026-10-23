CREATE TABLE demo_log_tasks (
    id BIGINT DEFAULT nextval('id_generator'::regclass) PRIMARY KEY,
    object_type VARCHAR,
    task_id BIGINT,
    in_progress BOOLEAN,
    tx_id VARCHAR
);

CREATE FUNCTION demo_log_tasks() RETURNS trigger AS $demo_log_tasks$ BEGIN
INSERT INTO demo_log_tasks (object_type, task_id, in_progress, tx_id)
VALUES ('task', NEW.id, NEW.in_progress, txid_current());
RETURN NEW;
END;
$demo_log_tasks$ LANGUAGE plpgsql;

CREATE TRIGGER demo_log_tasks_trigger
AFTER
UPDATE ON tasks FOR EACH ROW EXECUTE FUNCTION demo_log_tasks();
