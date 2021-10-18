package peterschrott.flink_stream_table_avro.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import peterschrott.flink_stream_table_avro.MyRecord;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class KafkaWriter {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        String topicName = "test.stream_table.avro";
        Properties props = getProducerProperties();
        Producer<String, MyRecord> producer = new KafkaProducer<>(props);

        for(int i = 0; i < 5; i++) {
            MyRecord myRecord = MyRecord.newBuilder()
                    .setText("free text " + i)
                    .build();

            ProducerRecord<String, MyRecord> record = new ProducerRecord<>(topicName, Integer.toString(i), myRecord);
            producer.send(record).get();
            System.out.println("Record produced" + record);
        }

        producer.close();
    }

    private static Properties getProducerProperties() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "...:9093");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("linger.ms", 1);

        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
        props.put("schema.registry.url", "https://...:8081");
        props.put("auto.register.schemas", "true");
        props.put("value.subject.name.strategy", "io.confluent.kafka.serializers.subject.RecordNameStrategy");
        return props;
    }
}