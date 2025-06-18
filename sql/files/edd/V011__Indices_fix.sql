DROP INDEX IF EXISTS command_store_target_breadcrumbs;


CREATE INDEX command_store_target_breadcrumbs
    ON command_store (request_id, target_breadcrumbs);

