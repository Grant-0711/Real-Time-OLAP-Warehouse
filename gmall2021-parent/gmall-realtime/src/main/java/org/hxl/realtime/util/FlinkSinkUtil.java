package org.hxl.realtime.util;
import com.alibaba.fastjson.JSONObject;
import org.hxl.realtime.bean.TableProcess;
import org.hxl.realtime.sink.PhoenixSink;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
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

}
