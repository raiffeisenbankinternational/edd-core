ALTER TABLE glms.event_store ADD COLUMN  invocation_id uuid;
ALTER TABLE glms.identity_store ADD COLUMN  invocation_id uuid;
ALTER TABLE glms.sequence_store ADD COLUMN  invocation_id uuid;

ALTER TABLE glms.command_store ADD COLUMN  invocation_id uuid;
ALTER TABLE glms.command_store ADD COLUMN  request_id uuid;
ALTER TABLE glms.command_store ADD COLUMN  interaction_id uuid;

ALTER TABLE glms.command_deps_log ADD COLUMN  invocation_id uuid;
ALTER TABLE glms.command_request_log ADD COLUMN  invocation_id uuid;
ALTER TABLE glms.command_response_log ADD COLUMN  invocation_id uuid;

