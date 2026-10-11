CREATE TABLE sims (
    id UUID PRIMARY KEY,
    task_id BIGINT NOT NULL,
    created TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    modified TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    json_data JSON NOT NULL
);
CREATE UNIQUE INDEX sims_task_idx ON sims (task_id);

CREATE TABLE buses (
    id UUID PRIMARY KEY,
    task_id BIGINT NOT NULL,
    created TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    modified TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    json_data JSON NOT NULL
);
CREATE UNIQUE INDEX buses_task_idx ON buses (task_id);
CREATE INDEX buses_simulation_idx ON buses ((json_data ->> 'simulationId'));
