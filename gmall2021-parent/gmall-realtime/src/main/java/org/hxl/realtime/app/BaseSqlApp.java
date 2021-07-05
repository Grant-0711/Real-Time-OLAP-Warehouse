package org.hxl.realtime.app;

/**
 * @author Grant
 * @create 2021-07-05 4:37
 */

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public abstract class BaseSqlApp {
    public abstract void run(StreamTableEnvironment tEnv);

    public void init(int port,
                     int p,
                     String ck) {  // 同时消费多个topic
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

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        run(tEnv);

        try {
            env.execute(ck);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
