package org.hxl.realtime.app;

/**
 * @author Grant
 * @create 2021-07-02 4:14
 */
import org.hxl.realtime.util.FlinkSourceUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

public abstract class BaseAppV2 {
    public abstract void run(StreamExecutionEnvironment env,
                             HashMap<String, DataStreamSource<String>> sourceStream);

    public void init(int port,
                     int p,
                     String ck,
                     String groupId,
                     String topic,
                     String... otherTopics) {  // 同时消费多个topic
        System.setProperty("HADOOP_USER_NAME", "grant");
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", port);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(p);

        // 设置状态后端
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop107:9820/flink-realtime/ck/" + ck);

        env.enableCheckpointing(3000, CheckpointingMode.EXACTLY_ONCE);

        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000);
        env.getCheckpointConfig()
                .enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // 具体的业务
        ArrayList<String> topics = new ArrayList<>(Arrays.asList(otherTopics));
        topics.add(topic);

        // 0 1 2
        HashMap<String, DataStreamSource<String>> topicAndStreamMap = new HashMap<>();
        for (String t : topics) {
            DataStreamSource<String> stream = env.addSource(FlinkSourceUtil.getKafkaSource(groupId, t));
            topicAndStreamMap.put(t, stream);
        }


        //        sourceStream.print();  // 不同的应用有不同的业务
        run(env, topicAndStreamMap);

        try {
            env.execute(ck);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
