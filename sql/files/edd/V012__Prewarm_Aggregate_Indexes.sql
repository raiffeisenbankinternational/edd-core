
create extension if not exists pg_prewarm;

create materialized view mv_aggregates_index_prewarm as
select
	pg_prewarm(schemaname || '.' || indexname) as blocks,
	(schemaname || '.' || indexname) as index
from pg_indexes
where schemaname like '%%_glms%_%'
order by 1 desc;

create extension if not exists pg_cron;

select cron.schedule(
    'cron_aggregates_index_prewarm',
    '*/15 * * * *', -- every 15 mintues
    'refresh materialized view mv_aggregates_index_prewarm'
);
