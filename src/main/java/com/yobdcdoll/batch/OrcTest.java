package com.yobdcdoll.batch;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;

/**
 * 从orc读取数据，自定义UDF、UDTF
 */
public class OrcTest {
    public static void main(String[] args) {
        EnvironmentSettings envSetting = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inBatchMode()
                .build();
        TableEnvironment env = TableEnvironment.create(envSetting);

        env.executeSql("create table movies (" +
                "movieId bigint" +
                ",title string" +
                ",genres string" +
                ") with (" +
                "'connector' = 'filesystem'" +
                ",'path' = 'input/movies.orc'" +
                ",'format' = 'orc'" +
                ")");

        env.createTemporarySystemFunction("upper_case", UpperCaseUDF.class);
        env.createTemporarySystemFunction("genreUDTF", GenreUDTF.class);

        Table genreTable = env.sqlQuery("select movieId, title, genre" +
                " from movies" +
                " left join lateral table(genreUDTF(genres))  AS T(genre) on true");
        env.createTemporaryView("t_genre", genreTable);
//        env.executeSql("select" +
//                " movieId" +
//                " ,upper_case(title) upper_title" +
//                " ,count(1) as total" +
//                " from t_genre" +
//                " group by movieId, title" +
//                " order by total asc" +
//                " limit 10")
//                .print();
//
//        env.executeSql("with t_genre2 as (" +
//                " select movieId, title, genre" +
//                " from movies" +
//                " left join lateral table(genreUDTF(genres))  AS T(genre) on true" +
//                " )" +
//                " select" +
//                " movieId" +
//                " ,upper_case(title) upper_title" +
//                " ,count(1) as total" +
//                " from t_genre2" +
//                " group by movieId, title" +
//                " order by total desc" +
//                " limit 10")
//                .print();

        env.createTemporarySystemFunction("mysum", MySumUDAF.class);
        env.executeSql("select genres, mysum(genres) as total from movies group by genres")
                .print();

    }
}
