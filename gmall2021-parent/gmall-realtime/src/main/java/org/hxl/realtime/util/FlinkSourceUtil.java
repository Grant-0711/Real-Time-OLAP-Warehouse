package org.hxl.realtime.util;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * @author Grant
 * @create 2021-06-25 17:32
 */
public class FlinkSourceUtil {
    public static FlinkKafkaConsumer<String> getKafkaSource(String groupId, String topic){
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "hadoop107:9092,hadoop108:9092,hadoop109:9092");
        props.setProperty("group.id", groupId);
        props.setProperty("auto.offset.reset", "latest");
        props.setProperty("isolation.level", "read_committed");

        return new FlinkKafkaConsumer<String>(
                topic,
                new SimpleStringSchema(),
                props

        );
    }
}
