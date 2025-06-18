DROP TABLE IF EXISTS sequence_lastval;
CREATE TABLE sequence_lastval (
    service_name VARCHAR(256),
    last_value BIGINT,
    PRIMARY KEY (service_name)
);

INSERT INTO sequence_lastval
SELECT service_name, MAX(value)
FROM sequence_store
GROUP BY service_name;
