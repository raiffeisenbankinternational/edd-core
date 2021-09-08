DROP TABLE command_request_log;

CREATE TABLE command_request_log (
    invocation_id uuid NOT NULL,
    request_id uuid NOT NULL,
    interaction_id uuid NOT NULL,
    breadcrumbs varchar(255) NOT NULL,
    cmd_index INT NOT NULL,
    created_on TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    service_name VARCHAR(256),
    receive_count INTEGER,
    data jsonb NOT NULL,
    error jsonb,
    PRIMARY KEY (request_id, breadcrumbs)
) PARTITION BY hash (request_id, breadcrumbs);

DO
$do$
DECLARE
   m CHARACTER VARYING := 'command_request_log';
BEGIN
   FOR i IN 0..31 LOOP
    raise info 'Creating % %', m, i;
    EXECUTE 'CREATE TABLE ' || 'part_pk_' || m || '_' || i || ' ' ||
            'PARTITION OF ' || m || ' '  ||
            'FOR VALUES WITH (MODULUS 32, REMAINDER ' || i || ')';
   END LOOP;
END
$do$;


