create extension if not exists "uuid-ossp";

create table if not exists aggregates (
    id UUID primary key,
    aggregate JSONB COMPRESSION lz4 not null,
    created_at timestamp with time zone not null default current_timestamp,
    updated_at timestamp with time zone null
);

create index if not exists idx_aggregates_created_at_desc
    on aggregates (created_at desc);

create index if not exists idx_aggregates_aggregate_gin_jsonb_path
    on aggregates using gin (aggregate jsonb_path_ops);
