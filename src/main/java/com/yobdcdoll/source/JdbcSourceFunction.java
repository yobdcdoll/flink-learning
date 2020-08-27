package com.yobdcdoll.source;

import com.yobdcdoll.util.JdbcType;
import com.yobdcdoll.util.PropConstant;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.types.Row;

import java.sql.*;
import java.util.*;

/**
 * 读取MySQL表的增量数据，最新记录的状态记录在BroadcastState，
 */
@Slf4j
public class JdbcSourceFunction extends RichSourceFunction<Row> implements CheckpointedFunction {
    private volatile boolean isRunning = true;
    private Connection connection = null;

    /**
     * 数据源配置
     */
    private String datasourceName;
    private String jdbcUrl;
    private String username;
    private String password;
    private String table;
    private String primaryKeys;

    /**
     * sql查询的时间间隔
     */
    private Long interval;

    private transient MapStateDescriptor<String, String> stateDescriptor;
    private transient BroadcastState<String, String> sourceState;
    private Map<String, String> realStateMap;

    public JdbcSourceFunction(String datasourceName) {
        this.datasourceName = datasourceName;
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
        this.table = parameterTool.get(PropConstant.append(datasourceName, PropConstant.TABLE));
        this.primaryKeys = parameterTool.get(PropConstant.append(datasourceName, PropConstant.PRIMARY_KEYS));
        this.interval = parameterTool.getLong(PropConstant.append(datasourceName, PropConstant.INTERVAL));

        Class.forName("com.mysql.jdbc.Driver");
        connection = DriverManager.getConnection(this.jdbcUrl, this.username, this.password);
    }

    @Override
    public void close() throws Exception {
        if (Objects.nonNull(connection)) {
            connection.close();
        }
    }

    @Override
    public void run(SourceContext<Row> sourceContext) throws Exception {
        List<String> keys = Arrays.asList(primaryKeys
                .replaceAll(" ", "")
                .split(","));


        while (isRunning) {
            synchronized (sourceContext.getCheckpointLock()) {
                Statement statement = connection.createStatement();
                StringBuffer sql = new StringBuffer("select * from " + table);
                if (realStateMap != null && !realStateMap.isEmpty()) {
                    sql.append(" where ");
                    boolean isFirstKey = true;
                    for (Map.Entry<String, String> entry : realStateMap.entrySet()) {
                        if (isFirstKey) {
                            isFirstKey = false;
                        } else {
                            sql.append(" and ");
                        }
                        sql.append(entry.getKey());
                        sql.append(">");
                        sql.append(entry.getValue());
                    }
                }
                statement.execute(sql.toString());
                log.info(sql.toString());
                ResultSet resultSet = statement.getResultSet();

                ResultSetMetaData rowMeta = resultSet.getMetaData();
                int colCount = rowMeta.getColumnCount();
                while (colCount >= 0 && resultSet.next()) {
                    String[] row = new String[4];
                    for (int i = 1; i <= colCount; i++) {
                        String colName = rowMeta.getColumnName(i);
                        String val = resultSet.getString(i);
                        int colType = rowMeta.getColumnType(i);

                        if (keys.indexOf(colName) >= 0 && (
                                !realStateMap.containsKey(colName)
                                        || JdbcType.compare(val, realStateMap.get(colName), colType) > 0
                        )) {
                            realStateMap.put(colName, val);
                        }
                        row[i - 1] = val;
                    }
                    sourceContext.collect(Row.of(row));
                }
            }

            Thread.sleep(interval);
        }
    }

    @Override
    public void cancel() {
        this.isRunning = false;
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        log.info("start JdbcSourceFunction#snapshotState()");
        if (Objects.nonNull(sourceState)) {
            sourceState.clear();
            sourceState.putAll(realStateMap);
            log.info("realStateMap contains {}", realStateMap);
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        stateDescriptor = new MapStateDescriptor<>(
                PropConstant.append(this.getClass().getName(), datasourceName)
                , String.class
                , String.class
        );

        sourceState = context.getOperatorStateStore().getBroadcastState(stateDescriptor);

        Iterable<Map.Entry<String, String>> entries = sourceState.entries();
        Iterator<Map.Entry<String, String>> it = entries.iterator();
        log.info("entries.iterator().hasNext() = " + entries.iterator().hasNext());

        realStateMap = new HashMap<>();
        while (it.hasNext()) {
            Map.Entry<String, String> next = it.next();
            realStateMap.put(next.getKey(), next.getValue());
        }
    }
}
