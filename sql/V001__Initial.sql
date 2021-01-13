CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE SCHEMA main;

CREATE table main.realm (
    id uuid PRIMARY KEY,
    created_on TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    data jsonb NOT NULL
);

INSERT INTO main.realm(id, created_on, data)
     VALUES (uuid_generate_v4(), NOW(), '{"name" : "default"}');


CREATE table main.event_store (
    id uuid,
    created_on TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    request_id uuid,
    interaction_id uuid,
    event_seq int NOT NULL,
    realm_id uuid,
    service varchar(255) NOT NULL,
    aggregate_id uuid,
    data jsonb NOT NULL,
    PRIMARY KEY (service, aggregate_id, event_seq)
);

CREATE INDEX main_event_store_aggregate_id ON main.event_store(service, aggregate_id);


CREATE table main.identity_store (
    realm_id uuid,
    created_on TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    service VARCHAR(254) NOT NULL,
    id VARCHAR(254) NOT NULL,
    aggregate_id UUID NOT NULL,
    PRIMARY KEY (aggregate_id, id),
    CONSTRAINT unique_per_service UNIQUE (service, id)
);

CREATE table main.sequence_store (
    realm_id uuid,
    aggregate_id UUID,
    service_name VARCHAR(254),
    created_on TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    value BIGINT,
    PRIMARY KEY (service_name, aggregate_id, value),
    UNIQUE (aggregate_id, service_name)
);

CREATE table main.command_store (
    realm_id uuid,
    id uuid PRIMARY KEY,
    created_on TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    service VARCHAR(256),
    data jsonb NOT NULL
);




CREATE table main.command_deps_log (
    realm_id uuid,
    request_id uuid,
    interaction_id uuid,
    cmd_index INT NOT NULL,
    created_on TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    service_name VARCHAR(256),
    data jsonb NOT NULL
);


CREATE table main.command_request_log (
    realm_id uuid,
    request_id uuid,
    interaction_id uuid,
    cmd_index INT NOT NULL,
    created_on TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    service_name VARCHAR(256),
    data jsonb NOT NULL
);

CREATE table main.command_response_log (
    realm_id uuid,
    request_id uuid,
    interaction_id uuid,
    cmd_index INT NOT NULL,
    created_on TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    service_name VARCHAR(256),
    data jsonb NOT NULL
);