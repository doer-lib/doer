CREATE SEQUENCE id_generator START WITH 1000 INCREMENT BY 1;

CREATE TABLE tasks (
    id BIGINT DEFAULT nextval('id_generator'::regclass) PRIMARY KEY,
    created TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    modified TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    status VARCHAR(50),
    in_progress BOOLEAN NOT NULL DEFAULT FALSE,
    failing_since TIMESTAMP WITH TIME ZONE,
    version INTEGER NOT NULL default 0
);

CREATE TABLE task_logs (
    id BIGINT DEFAULT nextval('id_generator'::regclass) PRIMARY KEY,
    task_id BIGINT NOT NULL,
    created TIMESTAMP WITH TIME ZONE DEFAULT now(),
    initial_status VARCHAR,
    final_status VARCHAR,
    class_name VARCHAR,
    method_name VARCHAR,
    duration_ms BIGINT,
    exception_type VARCHAR,
    extra_json JSON
);

CREATE INDEX IF NOT EXISTS tasks_status_idx ON tasks (status, created);
CREATE INDEX IF NOT EXISTS tasks_failing_idx ON tasks (status, modified) WHERE failing_since IS NOT NULL;
CREATE INDEX IF NOT EXISTS tasks_in_progress_idx ON tasks (status) WHERE in_progress;
