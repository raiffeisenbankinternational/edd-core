CREATE INDEX command_request_log_interaction
    ON command_request_log
    USING hash (interaction_id);

CREATE INDEX command_request_log_request_id
    ON command_request_log
    USING hash (request_id);


