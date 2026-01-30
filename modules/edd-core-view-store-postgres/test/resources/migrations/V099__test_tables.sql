-- Test-specific tables for integration tests
-- Version 099 ensures this runs AFTER production migrations (V001-V003)

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
