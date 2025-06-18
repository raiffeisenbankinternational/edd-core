
--
-- Various SQL expressions for development
--

create table if not exists test_glms_dimension_svc.dropdown1 (
    field_key text not null,
    field_val text not null
);

create index if not exists idx_dropdown1_key on test_glms_dimension_svc.dropdown1(field_key);

select field_val from test_glms_dimension_svc.dropdown1 where field_key = 'attrs.country-of-risk';

delete from test_glms_dimension_svc.dropdown1 where field_key = 'attrs.country-of-risk';


insert into test_glms_dimension_svc.dropdown1

select distinct
    'attrs.country-of-risk'                      as field_key,
    aggregate #>> array['attrs', 'country-of-risk'] as field_val
from test_glms_dimension_svc.aggregates
where
    aggregate #>> array['attrs', 'country-of-risk'] is not null
;



-------------

create table if not exists test_glms_dimension_svc.dropdown2 (
    field1_key text not null,
    field1_val text not null,
    field2_key text not null,
    field2_val text not null
);

drop table if exists test_glms_dimension_svc.dropdown2;

create index if not exists idx_dropdown2_keys on test_glms_dimension_svc.dropdown2(field1_key, field2_key);

select field1_val, field2_val from test_glms_dimension_svc.dropdown2 where field1_key = 'aaa' and field2_key = 'bb';

delete from test_glms_dimension_svc.dropdown2 where field1_key = 'aaa' and field2_key = 'bb';


insert into test_glms_dimension_svc.dropdown2

select distinct
    'attrs.gics-level-1-short-name'                      as field1_key,
    aggregate #>> array['attrs', 'gics-level-1-short-name'] as field1_val,
    'attrs.gics-level-3-short-name'                      as field2_key,
    aggregate #>> array['attrs', 'gics-level-3-short-name'] as field2_val
from test_glms_dimension_svc.aggregates
where
        aggregate #>> array['attrs', 'gics-level-1-short-name'] is not null
    and aggregate #>> array['attrs', 'gics-level-3-short-name'] is not null
;


--
-- Cleanup the environment
--

truncate
test_glms_currency_svc.aggregates,
test_glms_facility_svc.aggregates,
test_glms_application_svc.aggregates,
test_glms_dimension_svc.aggregates,
test_glms_exposure_svc.aggregates,
test_glms_application_requests_svc.aggregates,
test_glms_collateral_catalogue_svc.aggregates,
test_glms_product_set_svc.aggregates,
test_glms_notification_svc.aggregates

truncate
test_glms_access_rights_svc.aggregates,
test_glms_user_management_svc.aggregates

--
-- Bulk create schemas and tables
--

CREATE OR REPLACE FUNCTION init_service_schema(realm text, service_name text)
  RETURNS VOID
  LANGUAGE plpgsql AS
$func$
declare
    _schema text = format('%I_%I', realm, REPLACE(service_name, '-', '_'));
BEGIN
    EXECUTE FORMAT('create schema %I', _schema);

    EXECUTE FORMAT('
        create table if not exists %I.aggregates (
            id UUID primary key,
            aggregate JSONB COMPRESSION lz4 not null,
            created_at timestamp with time zone not null default current_timestamp,
            updated_at timestamp with time zone null
        )', _schema);

    EXECUTE FORMAT('
        create index if not exists
            idx_aggregates_aggregate_gin_jsonb_path
            ON %I.aggregates
            USING GIN (aggregate jsonb_path_ops)
    ', _schema);

END
$func$;


CREATE OR REPLACE FUNCTION drop_service_schema(realm text, service_name text)
  RETURNS VOID
  LANGUAGE plpgsql AS
$func$
declare
    _schema text = format('%I_%I', realm, REPLACE(service_name, '-', '_'));
BEGIN
    EXECUTE FORMAT('drop schema %I cascade', _schema);
END
$func$;



DO
$do$
DECLARE
    realm text := 'test';
    services text[] := ARRAY[
        'glms-access-rights-svc',
        'glms-actions-svc',
        'glms-api',
        'glms-application-requests-svc',
        'glms-application-svc',
        'glms-archive-svc',
        'glms-collateral-catalogue-svc',
        'glms-consolidation-svc',
        'glms-content-svc',
        'glms-cpa-proxy-svc',
        'glms-currency-svc',
        'glms-dataimport-svc',
        'glms-dimension-svc',
        'glms-document-svc',
        'glms-exposure-svc',
        'glms-facility-svc',
        'glms-group-limitations-svc',
        'glms-limit-review-report',
        'glms-mail-svc',
        'glms-notification-svc',
        'glms-nwu-api',
        'glms-plc2-svc',
        'glms-post-auth-trigger-svc',
        'glms-product-set-svc',
        'glms-remarks-svc',
        'glms-router-svc',
        'glms-task-manager-svc',
        'glms-template-svc',
        'glms-upload-svc',
        'glms-user-import-svc',
        'glms-user-management-svc'
   ];
   service text;
BEGIN
    FOREACH service IN ARRAY services
    LOOP
        PERFORM init_service_schema(realm, service);
        -- PERFORM drop_service_schema(realm, service);
    END LOOP;
END
$do$;


--
-- Select slow queries
-- 1. needs pg_stat_statements extension
-- 2. must be logged as postgres
--

select
	mean_exec_time,
	max_exec_time,
	query
from pg_stat_statements
where
    query like '%test_glms%'
and userid = 16399
and dbid = 5
order by
    max_exec_time desc
limit 100


--
-- Capture slow selects in CloudWatch
--

fields @timestamp
| filter @message like /END PG query.+SELECT/
| parse @message /END PG query: (?<query>.+?); elapsed\(msec\): (?<elapsed>[\d\.]+);/
| sort elapsed desc
| limit 5000


-- all queries

fields @timestamp
| filter @message like /END PG query/ # and @message like /dimension/
| parse @message /END PG query: (?<query>.+?); elapsed\(msec\): (?<elapsed>[\d\.]+);/
| sort elapsed desc
| limit 10000


--
-- Wildcard alerts
--

fields @timestamp
| filter @message like /PG ALERT WC/
| parse @message /attr: (?<attr>.+?), /
| parse @log /test-(?<service>.+?)$/
| sort timeout asc
| limit 10000


--
-- Order by alerts
--

fields @timestamp
| filter @message like /PG ALERT ORDER/
| parse @message /attr: (?<attr>.+?), /
| parse @log /test-(?<service>.+?)$/
| sort timeout asc
| limit 10000


--
-- Prewarm all the existing indexes
--

select
	pg_prewarm(schemaname || '.' || indexname) as blocks,
	(schemaname || '.' || indexname) as index
from pg_indexes
-- where tablename = 'aggregates'
where schemaname like '%%_glms%_%'
order by 1 desc


--
-- Dimension options
--

create table if not exists test_glms_dimension_svc.options_one_field (
	field_key text not null,
	field_val text not null
);

--;;

create index if not exists idx_options_one_field_key
	on test_glms_dimension_svc.options_one_field(field_key);

--;;

create table if not exists test_glms_dimension_svc.options_two_fields (
	field1_key text not null,
	field1_val text not null,
	field2_key text not null,
	field2_val text not null
);

--;;

create index if not exists idx_options_two_fields_keys
	on test_glms_dimension_svc.options_two_fields(field1_key, field2_key);





begin;

create or replace procedure proc_sync_options_one_field()
language sql
begin atomic

    delete from options_one_field;

    insert into options_one_field
    select distinct
    	'attrs.country-of-risk'                  	    as field_key,
    	aggregate #>> array['attrs', 'country-of-risk'] as field_val
    from
        aggregates
    where
    	aggregate #>> array['attrs', 'country-of-risk'] is not null;

end;

delete from test_glms_dimension_svc.options_one_field
where field_key = 'attrs.country-of-risk';

insert into test_glms_dimension_svc.options_one_field
select distinct
	'attrs.country-of-risk'                  	as field_key,
	aggregate #>> array['attrs', 'country-of-risk'] as field_val
from test_glms_dimension_svc.aggregates
where
	aggregate #>> array['attrs', 'country-of-risk'] is not null;

commit;


-------------

begin;

delete from test_glms_dimension_svc.options_two_fields
where
    	field1_key = 'attrs.asset-class.asset-class-code'
	and field2_key = 'attrs.asset-class.asset-class-name';

insert into test_glms_dimension_svc.options_two_fields
select distinct
	'attrs.asset-class.asset-class-code'                        	as field1_key,
	aggregate #>> array['attrs', 'asset-class', 'asset-class-code'] as field1_val,
	'attrs.asset-class.asset-class-name'                         	as field2_key,
	aggregate #>> array['attrs', 'asset-class', 'asset-class-name'] as field2_val
from test_glms_dimension_svc.aggregates
where
    	aggregate #>> array['attrs', 'asset-class', 'asset-class-code'] is not null
	and aggregate #>> array['attrs', 'asset-class', 'asset-class-name'] is not null;

commit;

---------

begin;

delete from test_glms_dimension_svc.options_two_fields
where
    	field1_key = 'attrs.gics-level-1-short-name'
	and field2_key = 'attrs.gics-level-3-short-name';

insert into test_glms_dimension_svc.options_two_fields
select distinct
	'attrs.gics-level-1-short-name'                     	as field1_key,
	aggregate #>> array['attrs', 'gics-level-1-short-name'] as field1_val,
	'attrs.gics-level-3-short-name'                     	as field2_key,
	aggregate #>> array['attrs', 'gics-level-3-short-name'] as field2_val
from test_glms_dimension_svc.aggregates
where
    	aggregate #>> array['attrs', 'gics-level-1-short-name'] is not null
	and aggregate #>> array['attrs', 'gics-level-3-short-name'] is not null;

commit;

----------

begin;

delete from test_glms_dimension_svc.options_two_fields
where
    	field1_key = 'attrs.pam-first-name'
	and field2_key = 'attrs.pam-last-name';

insert into test_glms_dimension_svc.options_two_fields
select distinct
	'attrs.pam-first-name'                  	as field1_key,
	aggregate #>> array['attrs', 'pam-first-name'] as field1_val,
	'attrs.pam-last-name'                   	as field2_key,
	aggregate #>> array['attrs', 'pam-last-name']  as field2_val
from test_glms_dimension_svc.aggregates
where
    	aggregate #>> array['attrs', 'pam-first-name'] is not null
	and aggregate #>> array['attrs', 'pam-last-name'] is not null;

commit;

----------

begin;

delete from test_glms_dimension_svc.options_two_fields
where
    	field1_key = 'attrs.cam-first-name'
	and field2_key = 'attrs.cam-last-name';

insert into test_glms_dimension_svc.options_two_fields
select distinct
	'attrs.cam-first-name'                  	as field1_key,
	aggregate #>> array['attrs', 'cam-first-name'] as field1_val,
	'attrs.cam-last-name'                   	as field2_key,
	aggregate #>> array['attrs', 'cam-last-name']  as field2_val
from test_glms_dimension_svc.aggregates
where
    	aggregate #>> array['attrs', 'cam-first-name'] is not null
	and aggregate #>> array['attrs', 'cam-last-name'] is not null;

commit;

----------
