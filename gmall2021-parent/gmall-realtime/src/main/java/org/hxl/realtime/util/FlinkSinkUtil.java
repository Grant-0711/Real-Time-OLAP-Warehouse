package org.hxl.realtime.util;
import com.alibaba.fastjson.JSONObject;
import org.hxl.realtime.annotation.NoSink;
import org.hxl.realtime.bean.TableProcess;
import org.hxl.realtime.common.Constant;
import org.hxl.realtime.sink.PhoenixSink;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;



/**
 * @author Grant
 * @create 2021-06-26 5:18
 */
public class FlinkSinkUtil {
    // 得到一个kafka source
    public static FlinkKafkaProducer<String> getKafkaSink(String topic) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "hadoop107:9092,hadoop108:9092,hadoop109:9092");
        props.setProperty("transaction.timeout.ms", 15 * 60 * 1000 + ""); // broker要求事务超时时间不能超过15分钟
        return new FlinkKafkaProducer<>(
                topic,
                (KafkaSerializationSchema<String>) (element, timestamp) -> new ProducerRecord<>(topic, null, element.getBytes(StandardCharsets.UTF_8)),
                props,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
    }

    public static SinkFunction<Tuple2<JSONObject, TableProcess>> getKafkaSink() {

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "hadoop107:9092,hadoop108:9092,hadoop109:9092");
        props.setProperty("transaction.timeout.ms", 15 * 60 * 1000 + ""); // broker要求事务超时时间不能超过15分钟



        return new FlinkKafkaProducer<>(
                "default",
                (KafkaSerializationSchema<Tuple2<JSONObject, TableProcess>>) (element, timestamp) -> {
                    String topic = element.f1.getSinkTable();
                    return new ProducerRecord<>(topic,
                            null,
                            element.f0.toJSONString().getBytes(StandardCharsets.UTF_8));
                },
                props,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE
        );
    }




    public static SinkFunction<Tuple2<JSONObject, TableProcess>> getHBaseSink() {
        // 自定义sink: 继承类RichSinkFunction
        return new PhoenixSink();
    }
    // 封装一个ck sink
    public static <T> SinkFunction<T> getClickhouseSink(String db,
                                                        String table,
                                                        Class<T> tClass) {
        String driver = Constant.CLICKHOUSE_DRIVER;
        String url = Constant.CLICKHOUSE_URL_PRE + db;

        StringBuilder sql = new StringBuilder();
        // 拼接一个向clickhouse写数据的sql
        // insert into t(a, b, c) values(?,?,?)
        sql.append("insert into ")
                .append(table)
                .append("(");
        // TODO 拼接字段名
        Field[] fields = tClass.getDeclaredFields();
        for (Field field : fields) {
            // 获取nosink注解, 如果为空, 则表示数据需要sink到clickhouse中
            NoSink noSink = field.getAnnotation(NoSink.class);
            if (noSink == null) {
                sql.append(field.getName()).append(",");
            }
        }
        sql.deleteCharAt(sql.length() - 1);
        sql.append(")values(");

        // TODO 根据前面字段的个数, 拼接 ?
        for (Field field : fields) {
            NoSink noSink = field.getAnnotation(NoSink.class);
            if (noSink == null) {
                sql.append("?,");
            }
        }
        sql.deleteCharAt(sql.length() - 1);
        sql.append(")");
        return getJdbcSink(driver, url, sql.toString());
    }


    public static <T> SinkFunction<T> getJdbcSink(String driver,
                                                  String url,
                                                  String sql) {
        return JdbcSink.sink(sql,
                new JdbcStatementBuilder<T>() {
                    @Override
                    public void accept(PreparedStatement ps,
                                       T t) throws SQLException {
                        System.out.println(t);
                        // 给sql中的占位符进行赋值
                        // insert into user(stt,edt,vc,ch,ar,is_new,uv_ct,pv_ct,sv_ct,uj_ct,dur_sum,ts)
                        // values(?,?,?,?,?,?,?,?,?,?,?,?)
                        Field[] fields = t.getClass().getDeclaredFields();
                        try {
                            for (int i = 0, position = 1; i < fields.length; i++) {
                                NoSink noSink = fields[i].getAnnotation(NoSink.class);
                                if (noSink == null) {
                                    fields[i].setAccessible(true); // 给属性添加访问权限
                                    Object v = fields[i].get(t); // 无法访问私有属性的值
                                    ps.setObject(position++, v);
                                }

                            }
                        } catch (IllegalAccessException e) {
                            e.printStackTrace();
                        }

                    }
                },
                new JdbcExecutionOptions.Builder()
                        .withBatchIntervalMs(1000)
                        .withBatchSize(1024)
                        .withMaxRetries(3)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withDriverName(driver)
                        .withUrl(url)
                        .build()
        );
    }

}

