-- History table for versioned aggregate snapshots
-- Stores each version of an aggregate for historical retrieval
-- Partitioned by hash(id, version) with configurable partition count
-- Default: 1 partition (no split). Override via Flyway placeholder historyPartitionCount.

create table if not exists aggregates_history (
    id UUID not null,
    version integer not null,
    data JSONB COMPRESSION lz4 not null,
    valid_from timestamp with time zone not null default current_timestamp,
    PRIMARY KEY (id, version)
) PARTITION BY hash (id, version);

-- Composite index for point-in-time queries:
-- SELECT ... WHERE id = ? AND valid_from < ? ORDER BY valid_from DESC LIMIT 1
create index if not exists idx_aggregates_history_id_valid_from
    on aggregates_history (id, valid_from desc);

DO
$do$
BEGIN
   FOR i IN 0..${historyPartitionCount} - 1 LOOP
     EXECUTE 'CREATE TABLE IF NOT EXISTS part_aggregates_history_' || i || ' ' ||
             'PARTITION OF aggregates_history ' ||
             'FOR VALUES WITH (MODULUS ${historyPartitionCount}, REMAINDER ' || i || ')';
   END LOOP;
END
$do$;
