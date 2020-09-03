package com.yobdcdoll.table;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

/**
 * 1、从hdfs上的csv文件作为表来源读取数据
 * 2、写入hive的orc表
 */
public class HiveTableTest {
    public static void main(String[] args) {
        EnvironmentSettings envSetting = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inBatchMode()
                .build();
        TableEnvironment env = TableEnvironment.create(envSetting);

        /**
         * 读取hdfs上的csv文件作为表
         */
        env.executeSql("create table movies(" +
                "movieId bigint" +
                ",title varchar" +
                ",genres varchar" +
                ") with (" +
                "'connector.type'='filesystem'" +
                ",'connector.path'='hdfs:///input/movies/movies.csv'" +
                ",'format.type'='csv'" +
                ",'format.ignore-first-line'='true'" +
                ",'format.field-delimiter'=','" +
                ",'format.line-delimiter'=U&'\\000A'" +
                ")");
        env.executeSql("create table tags(" +
                "userId bigint" +
                ",movieId bigint" +
                ",tag varchar" +
                ",timestamp_ bigint" +
                ") with (" +
                "'connector.type'='filesystem'" +
                ",'connector.path'='hdfs:///input/movies/tags.csv'" +
                ",'format.type'='csv'" +
                ",'format.ignore-first-line'='true'" +
                ",'format.field-delimiter'=','" +
                ",'format.line-delimiter'=U&'\\000A'" +
                ")");

        /**
         * 创建输出文件为orc
         */
        env.executeSql("create table movie_tags(" +
                "movieId bigint" +
                ",movieName varchar" +
                ",tagCount bigint" +
                ") with (" +
                "'connector.type'='filesystem'" +
                ",'connector.path'='hdfs:///output/movie_tags'" +
                ",'format.type'='orc'" +
                ")");

        /**
         * 事先在hive中创建orc表
         * create table movie_tags(movieId bigint, movieName string, tagCount bigint) stored as orc;
         */
        Table movieTagSource = env.sqlQuery("select tags.movieId, title movieName, count(1) tagCount from tags " +
                "left join movies on movies.movieId = tags.movieId " +
                "group by tags.movieId, movies.title " +
                "order by tagCount desc ");

        HiveCatalog hive = new HiveCatalog("myHive", "default", "/opt/tools/hive/conf");
        env.registerCatalog("myHive", hive);

        /**
         * 向hive表写入数据
         */
        movieTagSource.executeInsert("myHive.default.movie_tags", true);

        /**
         * 打印现有的catalogs
         */
        String[] catalogs = env.listCatalogs();
        for (int i = 0; catalogs != null & i < catalogs.length; i++) {
            System.out.println(catalogs[i]);
        }
    }
}
