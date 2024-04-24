
create schema if not exists test_glms_dimension_svc;

--;;

create table if not exists test_glms_dimension_svc.mv_options_one_field (
    field_key text not null,
    field_val text not null
);

--;;

create index if not exists idx_options_one_field_key
    on test_glms_dimension_svc.mv_options_one_field(field_key);

--;;

create table if not exists test_glms_dimension_svc.mv_options_two_fields (
    field1_key text not null,
    field1_val text not null,
    field2_key text not null,
    field2_val text not null
);

--;;

create index if not exists idx_options_two_fields_keys
    on test_glms_dimension_svc.mv_options_two_fields(field1_key, field2_key);
