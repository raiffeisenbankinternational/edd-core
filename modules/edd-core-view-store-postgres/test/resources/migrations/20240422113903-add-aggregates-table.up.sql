
create schema if not exists test_edd_core;

--;;

create table if not exists test_edd_core.aggregates (
    id UUID primary key,
    aggregate JSONB not null,
    created_at timestamp with time zone not null default current_timestamp,
    updated_at timestamp with time zone null
);

--;;

create index if not exists idx_aggregates_aggregate_gin_jsonb_path
    ON test_edd_core.aggregates USING GIN (aggregate jsonb_path_ops);

--;;

create schema if not exists test_glms_dimension_svc;

--;;

create table if not exists test_glms_dimension_svc.aggregates (
    id UUID primary key,
    aggregate JSONB not null,
    created_at timestamp with time zone not null default current_timestamp,
    updated_at timestamp with time zone null
);
