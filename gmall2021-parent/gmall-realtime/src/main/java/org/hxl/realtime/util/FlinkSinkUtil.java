package org.hxl.realtime.util;
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
    public static FlinkKafkaProducer<String> getKafkaSink(String topic){
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "hadoop162:9092,hadoop163:9092,hadoop164:9092");
        props.setProperty("transaction.timeout.ms", 15 * 60 * 1000 + ""); // broker要求事务超时时间不能超过15分钟
        return new FlinkKafkaProducer<String>(
                topic,
                new KafkaSerializationSchema<String>() {
                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(String element, @Nullable Long timestamp) {
                        return new ProducerRecord<>(topic, null, element.getBytes(StandardCharsets.UTF_8));
                    }
                },
                props,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
    }
}
