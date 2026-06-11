package com.bigdata.flink.config;

public class GlobalConfig {
    public static final String DRIVER_CLASS = "com.mysql.cj.jdbc.Driver";

    public static final String DB_URL = env(
            "DB_URL",
            "jdbc:mysql://mysql:3306/news?useUnicode=true&characterEncoding=UTF-8&useSSL=false&serverTimezone=Asia/Shanghai&allowPublicKeyRetrieval=true"
    );

    public static final String USER_MAME = env("DB_USER", "hive");

    public static final String PASSWORD = env("DB_PASSWORD", "bigdata123");

    public static String AUDITINSERTSQL = "insert into  auditcount (time,audit_type,province_code,count) VALUES (?,?,?,?) ON DUPLICATE KEY UPDATE time=VALUES(time),audit_type=VALUES(audit_type),province_code=VALUES(province_code),count=VALUES(count)";

    public static String env(String name, String defaultValue) {
        String value = System.getenv(name);
        return value == null || value.trim().isEmpty() ? defaultValue : value;
    }
}
