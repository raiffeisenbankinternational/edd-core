create table if not exists aggregates_history (
    id UUID,
    version integer not null,
    aggregate TEXT not null,
    service_name varchar(255) NOT NULL,
    valid_from timestamp with time zone not null default current_timestamp,
    valid_until timestamp with time zone,
    PRIMARY KEY (id, version, service_name)
) PARTITION BY hash (id, version, service_name);

create index if not exists idx_aggregates_history_valid_from_desc
    on aggregates_history (valid_from desc);

create index if not exists idx_aggregates_history_valid_until_desc
    on aggregates_history (valid_until desc);

DO
$do$
DECLARE
   m CHARACTER VARYING;
   arr varchar[] := array['aggregates_history'];
BEGIN

   FOREACH m IN ARRAY arr LOOP
     FOR i IN 0..31 LOOP
      raise info 'Creating % %', m, i;
      EXECUTE 'CREATE TABLE ' || 'part_' || m || '_' || i || ' ' ||
              'PARTITION OF ' || m || ' '  ||
              'FOR VALUES WITH (MODULUS 32, REMAINDER ' || i || ')';
     END LOOP;
   END LOOP;
END
$do$;
