DROP INDEX IF EXISTS test.command_store_target_breadcrumbs;


CREATE INDEX command_store_target_breadcrumbs
    ON test.command_store (request_id, target_breadcrumbs);

