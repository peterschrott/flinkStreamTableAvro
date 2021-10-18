CREATE TABLE my_table_from_kafka
(
    `text` STRING NOT NULL
) WITH (
    'connector' = 'kafka',
    'topic' = 'test.stream_table.avro',
    'scan.startup.mode' = 'earliest-offset',

    'properties.group.id' = 'flink-stream-table-test-001',
    'properties.bootstrap.servers' = 'localhost:9093',

    'value.format' = 'avro-confluent',
    'value.avro-confluent.schema-registry.url' = 'https://localhost:8081'
)
