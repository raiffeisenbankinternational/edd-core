CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

DROP TABLE IF EXISTS event_store;
CREATE TABLE event_store (
    id uuid,
    created_on TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    invocation_id uuid NOT NULL,
    request_id uuid NOT NULL,
    interaction_id uuid NOT NULL,
    breadcrumbs varchar(255) NOT NULL,
    event_seq int NOT NULL,
    service_name varchar(255) NOT NULL,
    aggregate_id uuid  NOT NULL,
    data jsonb NOT NULL,
    PRIMARY KEY (service_name, aggregate_id, event_seq)
) PARTITION BY hash (aggregate_id);

CREATE INDEX glms_event_store_aggregate_id ON event_store(service_name, aggregate_id);

DROP TABLE IF EXISTS identity_store;
CREATE TABLE identity_store (
    id VARCHAR(254) NOT NULL,
    created_on TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    service_name VARCHAR(254) NOT NULL,
    aggregate_id UUID NOT NULL,
    invocation_id uuid NOT NULL,
    request_id uuid NOT NULL,
    interaction_id uuid NOT NULL,
    breadcrumbs varchar(255) NOT NULL,
    PRIMARY KEY (aggregate_id, id),
    CONSTRAINT unique_per_service UNIQUE (service_name, id)
) PARTITION BY hash (id);


DROP TABLE IF EXISTS sequence_store;
CREATE TABLE sequence_store (
    aggregate_id UUID NOT NULL,
    service_name VARCHAR(254),
    created_on TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    invocation_id uuid NOT NULL,
    request_id uuid NOT NULL,
    interaction_id uuid NOT NULL,
    breadcrumbs varchar(255) NOT NULL,
    value BIGINT,
    PRIMARY KEY (service_name, aggregate_id, value),
    UNIQUE (aggregate_id, service_name)
) PARTITION BY hash (aggregate_id, service_name);

DROP TABLE IF EXISTS command_store;
CREATE TABLE command_store (
    id uuid NOT NULL,
    created_on TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    target_service VARCHAR(256) NOT NULL,
    source_service VARCHAR(256) NOT NULL,
    aggregate_id VARCHAR(256) NOT NULL,
    invocation_id uuid NOT NULL,
    request_id uuid NOT NULL,
    interaction_id uuid NOT NULL,
    breadcrumbs varchar(255) NOT NULL,
    data jsonb NOT NULL,
    PRIMARY KEY (request_id, breadcrumbs)
) PARTITION BY hash (request_id, breadcrumbs);



DROP TABLE IF EXISTS command_deps_log;
CREATE TABLE command_deps_log (
    invocation_id uuid NOT NULL,
    request_id uuid NOT NULL,
    interaction_id uuid NOT NULL,
    breadcrumbs varchar(255) NOT NULL,
    cmd_index INT NOT NULL,
    created_on TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    service_name VARCHAR(256),
    data jsonb NOT NULL
) PARTITION BY hash (invocation_id);


DROP TABLE IF EXISTS command_request_log;
CREATE TABLE command_request_log (
    invocation_id uuid NOT NULL,
    request_id uuid NOT NULL,
    interaction_id uuid NOT NULL,
    breadcrumbs varchar(255) NOT NULL,
    cmd_index INT NOT NULL,
    created_on TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    service_name VARCHAR(256),
    data jsonb NOT NULL
) PARTITION BY hash (invocation_id);

DROP TABLE IF EXISTS command_response_log;
CREATE TABLE command_response_log (
    invocation_id uuid NOT NULL,
    request_id uuid NOT NULL,
    interaction_id uuid NOT NULL,
    breadcrumbs varchar(255) NOT NULL,
    cmd_index INT NOT NULL,
    created_on TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    service_name VARCHAR(256),
    data jsonb NOT NULL,
    UNIQUE (request_id, breadcrumbs)
) PARTITION BY hash (request_id, breadcrumbs);

DO
$do$
DECLARE
   m CHARACTER VARYING;
   arr varchar[] := array['event_store',
                           'identity_store',
                           'sequence_store',
                           'command_store',
                           'command_deps_log',
                           'command_request_log',
                           'command_response_log'];
BEGIN

   FOREACH m IN ARRAY arr LOOP
     FOR i IN 0..31 LOOP
      raise info 'Creating % %', m, i;
      EXECUTE 'CREATE TABLE ' || 'part_' || m || '_' || i || ' ' ||
              'PARTITION OF ' || m || ' '  ||
              'FOR VALUES WITH (MODULUS 32, REMAINDER ' || i || ')';
     END LOOP;
   END LOOP;
END
$do$;
