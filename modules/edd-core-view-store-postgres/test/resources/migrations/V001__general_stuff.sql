
create table if not exists aggregates (
    id UUID primary key,
    aggregate JSONB not null,
    created_at timestamp with time zone not null default current_timestamp,
    updated_at timestamp with time zone null
);

create index if not exists idx_aggregates_aggregate_gin_jsonb_path
    ON aggregates USING GIN (aggregate jsonb_path_ops);


create table if not exists mv_options_one_field (
    field_key text not null,
    field_val text not null
);

create index if not exists idx_options_one_field_key
    on mv_options_one_field(field_key);

create table if not exists mv_options_two_fields (
    field1_key text not null,
    field1_val text not null,
    field2_key text not null,
    field2_val text not null
);

create index if not exists idx_options_two_fields_keys
    on mv_options_two_fields(field1_key, field2_key);
