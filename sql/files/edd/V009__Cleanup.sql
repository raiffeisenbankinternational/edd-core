DROP INDEX command_response_log_interaction_id;
DROP INDEX command_response_log_request_id;


ALTER TABLE command_response_log
 DROP COLUMN fx_exception, 
 DROP COLUMN fx_remaining, 
 DROP COLUMN fx_total, 
 ADD COLUMN fx_processed integer DEFAULT 0, 
 ADD COLUMN fx_created integer DEFAULT 0;


CREATE INDEX command_response_log_request_id
    ON command_response_log (request_id,
                             breadcrumbs,
                             fx_processed,
                             fx_created,
                             fx_error);

CREATE INDEX command_response_log_interaction_id
    ON command_response_log (interaction_id,
                             fx_processed,
                             fx_created,
                             fx_error);

ALTER TABLE command_request_log
 ADD COLUMN fx_exception integer DEFAULT 0; 

CREATE INDEX command_request_log_interaction_count
ON command_request_log (interaction_id, fx_exception);
