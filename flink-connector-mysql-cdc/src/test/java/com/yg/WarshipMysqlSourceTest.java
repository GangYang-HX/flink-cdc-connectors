package com.yg;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.Test;

public class WarshipMysqlSourceTest {

    @Test
    public void testConsumingMysql() throws Exception {
        MySqlSource<String> mySqlSource =
                MySqlSource.<String>builder()
                        .hostname("localhost")
                        .port(3306)
                        .databaseList("warship")
                        .tableList("warship.app_info")
                        .username("root")
                        .password("yg123456")
//                        .serverId("5401-5404")
                        .deserializer(new JsonDebeziumDeserializationSchema())
                        .includeSchemaChanges(true) // output the schema changes as well
                        .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // enable checkpoint
        env.enableCheckpointing(30000);
        // set the source parallelism to 4
        env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySqlParallelSource")
                .setParallelism(4)
                .print()
                .setParallelism(1);

        env.execute("Print MySQL Snapshot + Binlog");
    }
}
