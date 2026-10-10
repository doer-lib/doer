CREATE INDEX IF NOT EXISTS tasks_status_idx ON tasks (status, created);
CREATE INDEX IF NOT EXISTS tasks_failing_idx ON tasks (status, modified) WHERE failing_since IS NOT NULL;
CREATE INDEX IF NOT EXISTS tasks_in_progress_idx ON tasks (status) WHERE in_progress;
CREATE INDEX IF NOT EXISTS tasks_delayed_idx ON tasks (status, modified) WHERE status IN (
  'Bus at stop',
  'Bus at terminal',
  'Bus driving',
  'Doer method delayed',
  'Sim running'
);

