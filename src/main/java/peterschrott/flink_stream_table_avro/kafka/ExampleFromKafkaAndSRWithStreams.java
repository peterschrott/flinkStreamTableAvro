package peterschrott.flink_stream_table_avro.kafka;

import peterschrott.flink_stream_table_avro.MyRecord;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Properties;

public class ExampleFromKafkaAndSRWithStreams {
  
  public static String SCHEMA_REG_URL = "https://...:8081";
  public static String TOPIC = "test.stream_table.avro";

  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
    DataStreamSource<MyRecord> streamSource = getKafkaStreamSource(streamEnv);

    StreamTableEnvironment streamTableEnv = StreamTableEnvironment.create(streamEnv);

    Table inputTable = streamTableEnv.fromDataStream(streamSource);
    streamTableEnv.createTemporaryView("my_table_from_kafka", inputTable);
    Table resultTable = streamTableEnv.sqlQuery("SELECT * FROM my_table_from_kafka WHERE text LIKE '%free%'");

    streamTableEnv.toDataStream(resultTable).print();

    streamEnv.execute();
  }

  private static DataStreamSource<MyRecord> getKafkaStreamSource(StreamExecutionEnvironment env) {
    ConfluentRegistryAvroDeserializationSchema<MyRecord> deserializer =
            ConfluentRegistryAvroDeserializationSchema.forSpecific(MyRecord.class, SCHEMA_REG_URL);

    Properties props = new Properties();

    props.put("bootstrap.servers", "...:9093");
    props.put("client.id", "flink-stream-table-test");
    props.put("group.id", "flink-stream-table-test-001");

    props.put("schema.registry.url", "https://...:8081");


    KafkaSource<MyRecord> source = KafkaSource.<MyRecord>builder()
            .setProperties(props)
            .setTopics(TOPIC)
            .setValueOnlyDeserializer(deserializer)
            .build();

    return env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
  }
}