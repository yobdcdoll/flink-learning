package com.yobdcdoll.table;

import com.yobdcdoll.source.JdbcSourceTest;
import com.yobdcdoll.util.PropConstant;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * kafka表为order事件，users为mysql维度表
 * users表缓存1min
 */
public class KafkaTableTest {
    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool = ParameterTool.fromPropertiesFile(
                JdbcSourceTest.class.getResourceAsStream(PropConstant.APPLICATION_PROPERTIES)
        ).mergeWith(ParameterTool.fromArgs(args));

        String bootstrapServers = parameterTool.get("kafka.bootstrap.servers");
        String groupId = parameterTool.get("kafka.group.id");
        String topic = parameterTool.get("kafka.topic");

        EnvironmentSettings envSetting = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment env = StreamTableEnvironment.create(streamEnv, envSetting);

        env.executeSql("create table orders (" +
                "orderId bigint" +
                ",userId bigint" +
                ",itemId bigint" +
                ",createTime bigint" +
                ",proc_time as proctime()"+
//                ",eventTime as to_timestamp(from_unixtime(createTime/1000, 'yyyy-MM-dd HH:m:ss'))"+
//                ",watermark for eventTime as eventTime - interval '5' second"+
                ") with (" +
                "'connector' = 'kafka'" +
                ",'topic' = '" + topic + "'" +
                ",'properties.bootstrap.servers' = '" + bootstrapServers + "'" +
                ",'properties.group.id' = '" + groupId + "'" +
                ",'format' = 'csv'" +
                ",'scan.startup.mode' = 'earliest-offset'"+
                ")");

        env.executeSql("create table users (" +
                "userId bigint" +
                ",name string" +
                ",age int" +
                ",city string" +
//                ",primary key (userId) not enforced" +
                ") with (" +
                "'connector' = 'jdbc'" +
                ",'url' = 'jdbc:mysql://localhost:3306/dbmeta'" +
                ",'table-name' = 'users'" +
                ",'username'='root'" +
                ",'password'='root'" +
                ",'lookup.cache.max-rows'='1000'" +
                ",'lookup.cache.ttl'='1min'" +
                ")");

        Table orders = env.sqlQuery("select orders.orderId" +
                ", orders.itemId" +
                ", orders.userId" +
                ", users.name" +
                ", users.age" +
                ", users.city" +
                " from orders" +
                " left join users" +
                " for system_time as of orders.proc_time" +
                " on users.userId = orders.userId");
        DataStream<Tuple2<Boolean, Row>> outStream = env.toRetractStream(orders, Row.class);
        outStream.print();
        streamEnv.execute("Print kafka table");
    }
}
