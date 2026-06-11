package com.bigdata.flink.sink;

import com.bigdata.flink.config.GlobalConfig;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class MySQLSink2 extends RichSinkFunction<Tuple2<String, Integer>> {
    private Connection conn;
    private PreparedStatement statement;

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName(GlobalConfig.DRIVER_CLASS);
        conn = DriverManager.getConnection(
                GlobalConfig.DB_URL,
                GlobalConfig.USER_MAME,
                GlobalConfig.PASSWORD
        );
        statement = conn.prepareStatement(
                "insert into periodcount(logtime,count) values(?,?) "
                        + "on duplicate key update count=values(count)"
        );
    }

    @Override
    public void invoke(Tuple2<String, Integer> value, Context context) throws Exception {
        statement.setString(1, value.f0);
        statement.setInt(2, value.f1);
        statement.executeUpdate();
    }

    @Override
    public void close() throws Exception {
        if (statement != null) {
            statement.close();
        }
        if (conn != null) {
            conn.close();
        }
    }
}
