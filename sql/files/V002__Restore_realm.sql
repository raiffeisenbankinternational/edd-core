DROP TABLE IF EXISTS glms.realm;

CREATE table glms.realm (
    id uuid PRIMARY KEY,
    created_on TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    data jsonb NOT NULL
);

INSERT INTO glms.realm(id, created_on, data)
     VALUES (uuid_generate_v4(), NOW(), '{"name" : "default"}');

