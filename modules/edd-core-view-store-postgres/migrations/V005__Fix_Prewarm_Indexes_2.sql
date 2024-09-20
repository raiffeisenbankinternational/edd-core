
-- remove the previous broken cron job
select cron.unschedule('cron_aggregates_index_prewarm');

-- ensure materialization works
refresh materialized view ${flyway:defaultSchema}.mv_aggregates_index_prewarm;

-- set the right command for the job
select cron.schedule(
    '${flyway:defaultSchema}_cron_aggregates_index_prewarm',
    '*/15 * * * *', -- every 15 mintues
    'refresh materialized view ${flyway:defaultSchema}.mv_aggregates_index_prewarm'
);
