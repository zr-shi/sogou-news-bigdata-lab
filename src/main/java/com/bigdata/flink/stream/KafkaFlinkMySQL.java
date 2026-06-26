package com.bigdata.flink.stream;

import com.bigdata.flink.sink.MySQLSink;
import com.bigdata.flink.sink.MySQLSink2;
import com.bigdata.flink.config.GlobalConfig;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;


import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class KafkaFlinkMySQL {
    public static void main(String[] args) throws Exception {
        //获取flink执行环境
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        senv.getConfig().setAutoWatermarkInterval(200);
        //配置kafka集群参数
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers", GlobalConfig.env("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"));
        prop.setProperty("group.id", GlobalConfig.env("KAFKA_GROUP_ID", "sougoulogs"));

        //读取kafka数据
        String topic = GlobalConfig.env("KAFKA_TOPIC", "sougoulogs");
        FlinkKafkaConsumer<String> myConsumer = new FlinkKafkaConsumer<String>(topic,new SimpleStringSchema(),prop);
        myConsumer.setStartFromEarliest();
        DataStream<String> stream = senv.addSource(myConsumer);

        //数据过滤：兼容逗号、中文逗号、竖线、制表符、空格分隔以及带引号标题。
        DataStream<String> filter = stream.filter((value) -> LogRecord.parse(value) != null);

        //统计新闻话题访问量
        DataStream<Tuple2<String,Integer>> newsCounts = filter.flatMap(new lineSplitter())
                .keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Integer> t) throws Exception {
                        return t.f0;
                    }
                }).sum(1);
        newsCounts.print();
        //数据入库MySQL
        newsCounts.addSink(new MySQLSink());

        //统计每个时段新闻话题访问量
        DataStream<Tuple2<String,Integer>> periodCounts =filter.flatMap(new lineSplitter2())
                .keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Integer> t) throws Exception {
                        return t.f0;
                    }
                }).sum(1);
        periodCounts.addSink(new MySQLSink2());

        //执行flink程序
        senv.execute("KafkaFlinkMySQL");
    }


    public static final class lineSplitter implements FlatMapFunction<String, Tuple2<String,Integer>>{
        @Override
        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
            LogRecord record = LogRecord.parse(s);
            if (record != null) {
                collector.collect(new Tuple2<>(record.title, 1));
            }
        }
    }

    public static final class lineSplitter2 implements FlatMapFunction<String, Tuple2<String,Integer>>{
        @Override
        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
            LogRecord record = LogRecord.parse(s);
            if (record != null) {
                collector.collect(new Tuple2<>(record.logTime, 1));
            }
        }
    }

    static final class LogRecord {
        private static final Pattern TIME_PATTERN = Pattern.compile("(\\d{1,2})[:：](\\d{1,2})(?:[:：](\\d{1,2}))?");

        final String logTime;
        final String title;

        LogRecord(String logTime, String title) {
            this.logTime = logTime;
            this.title = title;
        }

        static LogRecord parse(String raw) {
            if (raw == null) {
                return null;
            }

            String line = raw.trim();
            if (line.isEmpty()) {
                return null;
            }

            line = line.replace("\uFEFF", "");

            List<String> fields = parseCsv(line);
            if (fields.size() != 6) {
                fields = parseCsv(line.replaceAll("\\s*(\\|\\||\\|)\\s*", ","));
            }
            if (fields.size() != 6) {
                fields = parseCsv(line.replaceAll("\\s*[，、]\\s*", ","));
            }
            if (fields.size() != 6) {
                fields = splitWhitespace(line);
            }
            if (fields.size() != 6) {
                return null;
            }

            String logTime = normalizeTime(clean(fields.get(0)));
            String title = clean(fields.get(2));
            if (logTime.isEmpty() || title.isEmpty()) {
                return null;
            }
            return new LogRecord(logTime, title);
        }

        private static List<String> splitWhitespace(String line) {
            String[] tokens = line.trim().split("\\s+", 6);
            List<String> fields = new ArrayList<>();
            for (String token : tokens) {
                fields.add(token);
            }
            return fields;
        }

        private static List<String> parseCsv(String line) {
            List<String> fields = new ArrayList<>();
            StringBuilder current = new StringBuilder();
            boolean quoted = false;
            for (int i = 0; i < line.length(); i++) {
                char ch = line.charAt(i);
                if (ch == '"') {
                    if (quoted && i + 1 < line.length() && line.charAt(i + 1) == '"') {
                        current.append('"');
                        i++;
                    } else {
                        quoted = !quoted;
                    }
                } else if (ch == ',' && !quoted) {
                    fields.add(current.toString().trim());
                    current.setLength(0);
                } else {
                    current.append(ch);
                }
            }
            fields.add(current.toString().trim());
            return fields;
        }

        private static String clean(String value) {
            String cleaned = value == null ? "" : value.trim();
            while ((cleaned.startsWith("\"") && cleaned.endsWith("\""))
                    || (cleaned.startsWith("'") && cleaned.endsWith("'"))
                    || (cleaned.startsWith("“") && cleaned.endsWith("”"))
                    || (cleaned.startsWith("《") && cleaned.endsWith("》"))) {
                cleaned = cleaned.substring(1, cleaned.length() - 1).trim();
            }
            return cleaned.replaceAll("[\\[\\]]", "");
        }

        private static String normalizeTime(String value) {
            Matcher matcher = TIME_PATTERN.matcher(value);
            if (!matcher.find()) {
                return "";
            }
            int hour = Integer.parseInt(matcher.group(1)) % 24;
            int minute = Integer.parseInt(matcher.group(2)) % 60;
            int second = matcher.group(3) == null ? 0 : Integer.parseInt(matcher.group(3)) % 60;
            return String.format("%02d:%02d:%02d", hour, minute, second);
        }
    }
}
