package com.yobdcdoll.async;

import com.yobdcdoll.model.Order;
import com.yobdcdoll.util.PropConstant;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.SQLClient;
import io.vertx.ext.sql.SQLConnection;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.util.ArrayList;
import java.util.List;

public class OrderUserAsyncFunction extends RichAsyncFunction<Order
        , Tuple6<Long, Long, String, Integer, String, Long>> {

    /**
     * 数据源配置
     */
    private String datasourceName;
    private String jdbcUrl;
    private String username;
    private String password;

    /**
     * 异步请求jdbc客户端
     */
    private SQLClient client;

    public OrderUserAsyncFunction(String datasourceName) {
        this.datasourceName = datasourceName;
    }

    @Override
    public void asyncInvoke(Order input
            , ResultFuture<Tuple6<Long, Long, String, Integer, String, Long>> resultFuture
    ) throws Exception {
        client.getConnection(new Handler<AsyncResult<SQLConnection>>() {
            @Override
            public void handle(AsyncResult<SQLConnection> conn) {
                if (conn.failed()) {
                    return;
                }
                final SQLConnection connection = conn.result();
                String sql = "select * from users where userId = " + input.getUserId();
                connection.query(sql, new Handler<AsyncResult<ResultSet>>() {
                    @Override
                    public void handle(AsyncResult<ResultSet> result) {
                        ResultSet rs = new ResultSet();
                        if (result.succeeded()) {
                            rs = result.result();
                        }
                        List<Tuple6<Long, Long, String, Integer, String, Long>> stores = new ArrayList<>();
                        for (JsonObject json : rs.getRows()) {
                            Long userId = json.getLong("userId");
                            String name = json.getString("name");
                            Integer age = json.getInteger("age");
                            String city = json.getString("city");
                            Tuple6<Long, Long, String, Integer, String, Long> outItem = Tuple6.of(
                                    input.getItemId()
                                    , input.getUserId()
                                    , name
                                    , age
                                    , city
                                    , input.getCreateTime()
                            );

                            stores.add(outItem);
                        }
                        connection.close();
                        resultFuture.complete(stores);
                    }
                });
            }
        });
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        ParameterTool parameterTool =
                (ParameterTool) getRuntimeContext()
                        .getExecutionConfig()
                        .getGlobalJobParameters();
        /**
         * 获取数据源连接的配置参数
         */
        this.jdbcUrl = parameterTool.get(PropConstant.append(datasourceName, PropConstant.JDBC_URL));
        this.username = parameterTool.get(PropConstant.append(datasourceName, PropConstant.USERNAME));
        this.password = parameterTool.get(PropConstant.append(datasourceName, PropConstant.PASSWORD));

        JsonObject config = new JsonObject()
                .put("url", jdbcUrl)
                .put("driver_class", "com.mysql.jdbc.Driver")
                .put("max_pool_size", 10)
                .put("user", username)
                .put("password", password);
        client = JDBCClient.createShared(Vertx.vertx(), config);
    }

    @Override
    public void close() throws Exception {
        if (client != null) {
            client.close();
        }
    }
}
