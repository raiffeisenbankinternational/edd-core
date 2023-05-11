ALTER TABLE command_store
  ADD COLUMN target_breadcrumbs varchar(255);

ALTER TABLE command_response_log
 ADD COLUMN fx_last_on TIMESTAMPTZ NOT NULL DEFAULT NOW(); 

CREATE INDEX command_response_log_request_id
    ON command_response_log (request_id, breadcrumbs);

CREATE INDEX command_response_log_interaction_id
    ON command_response_log (interaction_id);


