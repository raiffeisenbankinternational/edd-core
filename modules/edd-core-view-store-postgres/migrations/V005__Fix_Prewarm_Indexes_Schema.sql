
do $$
declare
    cron_job text = 'cron_aggregates_index_prewarm';
begin
    select cron.unschedule(cron_job);
    exception
        when others then -- don't know the exact error code
            raise notice 'could not remove the cron job %s', cron_job;
end;
$$;

-- ensure materialization works
refresh materialized view ${flyway:defaultSchema}.mv_aggregates_index_prewarm;

-- set the right command for the job
select cron.schedule(
    '${flyway:defaultSchema}_cron_aggregates_index_prewarm',
    '*/15 * * * *', -- every 15 mintues
    'refresh materialized view ${flyway:defaultSchema}.mv_aggregates_index_prewarm'
);
