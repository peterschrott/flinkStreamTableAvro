- src/main/avro/MyProtocol.avdl
    - MyRecord is defined
    - "mvn generate-sources" will generate Java classes under "target/generated-sources"
- "src/main/java/peterschrott/flink-stream_table_avro" contains
    - "KafkaWriter.java" which writes the following record of MyRecord type to Kafka topic "test.stream_table.avro"
        - {"text": "free text 1"}
    - "ExampleFromKafkaAndSRWithStreams.java" creates a KafkaSource, consumes the topic "test.stream_table.avro" and converts the data stream into a table