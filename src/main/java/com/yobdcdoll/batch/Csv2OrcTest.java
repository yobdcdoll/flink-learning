package com.yobdcdoll.batch;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.sources.CsvTableSource;

public class Csv2OrcTest {
    public static void main(String[] args) {
        EnvironmentSettings envSetting = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inBatchMode()
                .build();
        TableEnvironment env = TableEnvironment.create(envSetting);

        CsvTableSource csvSource = CsvTableSource.builder()
                .path("input/movies.csv")
                .ignoreFirstLine()
                .field("movieId", DataTypes.BIGINT())
                .field("title", DataTypes.STRING())
                .field("genres", DataTypes.STRING())
                .build();
        env.registerTableSource("movies", csvSource);

        env.executeSql("create table movies_orc (" +
                "movieId bigint" +
                ",title string" +
                ",genres string" +
                ") with (" +
                "'connector' = 'filesystem'" +
                ",'path' = 'input/movies.orc'" +
                ",'format' = 'orc'" +
                ")");

        env.executeSql("insert into movies_orc select * from movies");
//        result.print();
    }
}
