CREATE EXTENSION IF NOT EXISTS pg_prewarm WITH SCHEMA public CASCADE;
CREATE EXTENSION IF NOT EXISTS pg_cron;

SELECT extname AS extension_name,
       nspname AS schema_name
FROM pg_extension
JOIN pg_namespace ON pg_extension.extnamespace = pg_namespace.oid;

DROP MATERIALIZED VIEW IF EXISTS mv_aggregates_index_prewarm;

create materialized view mv_aggregates_index_prewarm as
select
	pg_prewarm(schemaname || '.' || indexname) as blocks,
	(schemaname || '.' || indexname) as index
from pg_indexes
where schemaname = '${flyway:defaultSchema}'
order by 1 desc;

select cron.schedule(
    'cron_aggregates_index_prewarm',
    '*/15 * * * *', -- every 15 mintues
    'refresh materialized view {flyway:defaultSchema}mv_aggregates_index_prewarm'
);
