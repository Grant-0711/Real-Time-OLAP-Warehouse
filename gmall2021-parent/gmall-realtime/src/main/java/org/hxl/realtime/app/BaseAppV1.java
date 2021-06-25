package org.hxl.realtime.app;

/**
 * @author Grant
 * @create 2021-06-21 19:20
 */
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.hxl.realtime.util.FlinkSourceUtil;

public abstract class BaseAppV1 {
    public abstract void run(StreamExecutionEnvironment env, DataStreamSource<String> sourceStream );

    public void init(int port,
                     int p,
                     String ck,
                     String groupId,
                     String topic) {
        System.setProperty("HADOOP_USER_NAME", "grant");
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", port);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(p);

        // 设置状态后端
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://8.134.105.70:9820/flink-realtime/ck/" + ck);

        env.enableCheckpointing(3000, CheckpointingMode.EXACTLY_ONCE);

        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000);
        env.getCheckpointConfig()
                .enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // 具体的业务
        DataStreamSource<String> sourceStream = env
                .addSource(FlinkSourceUtil.getKafkaSource(groupId, topic));

//        sourceStream.print();  // 不同的应用有不同的业务
        run(env, sourceStream);

        try {
            env.execute(ck);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
