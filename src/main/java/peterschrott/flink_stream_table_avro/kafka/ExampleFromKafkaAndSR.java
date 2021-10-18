package peterschrott.flink_stream_table_avro.kafka;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class ExampleFromKafkaAndSR {

  public static void main(String[] args) throws Exception {
    Path ddlPath = Paths.get(new File("my_table_from_kafka.ddl").toURI());
    String ddl = Files.readString(ddlPath);
    System.out.println(ddl);

    EnvironmentSettings envSettings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build();

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    StreamTableEnvironment streamTableEnv = StreamTableEnvironment.create(env);

    TableEnvironment tEnv = TableEnvironment.create(envSettings);

    tEnv.executeSql(ddl);

    Table t = tEnv.sqlQuery("SELECT * FROM my_table_from_kafka WHERE text LIKE '%fre%'");

    streamTableEnv.toDataStream(t)
      .print();

    env.execute();
  }
}
