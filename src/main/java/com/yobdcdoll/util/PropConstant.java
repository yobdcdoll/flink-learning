package com.yobdcdoll.util;

import java.util.Objects;

public class PropConstant {
    public final static String APPLICATION_PROPERTIES = "/application.properties";
    public final static String CHECKPOINT_DIR = "checkpoint.dir";

    public final static String JDBC_URL = "jdbcUrl";
    public final static String USERNAME = "username";
    public final static String PASSWORD = "password";
    public final static String TABLE = "table";
    public final static String PRIMARY_KEYS = "primaryKeys";
    public final static String INTERVAL = "interval";

    public static String append(String left, String right) {
        Objects.requireNonNull(left);
        Objects.requireNonNull(right);
        return left + "." + right;
    }
}
