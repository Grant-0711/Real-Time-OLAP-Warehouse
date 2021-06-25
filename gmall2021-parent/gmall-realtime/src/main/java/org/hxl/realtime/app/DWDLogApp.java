package org.hxl.realtime.app;

/**
 * @author Grant
 * @create 2021-06-21 19:22
 */
import com.alibaba.fastjson.JSON;
import org.hxl.realtime.app.BaseApp;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
public class DWDLogApp extends BaseApp {
    public static void main(String[] args) {
        new DWDLogApp().init(1, "DWDLogApp", "ods_log");
    }

    @Override
    protected void run(StreamExecutionEnvironment env,
                       DataStreamSource<String> sourceStream) {

        // 1. 识别新老客户: 在日志数据中添加一个字段用来表示是否新老客户
        SingleOutputStreamOperator<JSONObject> validateFlagDS = distinguishNewOrOld(sourceStream);
        // 2. 流的拆分: 利用侧输出流

        // 3. 分别写入到不同的Topic中
    }

    private SingleOutputStreamOperator<JSONObject> distinguishNewOrOld(DataStreamSource<String> sourceStream) {
        SingleOutputStreamOperator<JSONObject> result = sourceStream
                // 数据解析成JSONObject对象
                .map(JSON::parseObject)
                .assignTimestampsAndWatermarks(  // 添加水印和事件时间
                        WatermarkStrategy
                                .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((element, recordTimestamp) -> element.getLong("ts"))
                )
                // 按照mid进行分组
                .keyBy(jsonData -> jsonData.getJSONObject("common").getString("mid"))
                .window(TumblingEventTimeWindows.of(Time.seconds(5))) // 添加滚动窗口
                .process(new ProcessWindowFunction<JSONObject, JSONObject, String, TimeWindow>() {

                    private ValueState<Long> firstVisitState;  // 存储是是首次访问的时间戳

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        firstVisitState = getRuntimeContext()
                                .getState(new ValueStateDescriptor<>("firstVisitState", Long.class));
                    }

                    @Override
                    public void process(String mid,
                                        Context context,
                                        Iterable<JSONObject> elements,
                                        Collector<JSONObject> out) throws Exception {
                        if (firstVisitState.value() == null) {
                            // 1.  如果是这个 mid 的首个窗口, 则把时间戳最小的设置首次访问, 其他均为非首次访问
                            ArrayList<JSONObject> list = new ArrayList<>();
                            for (JSONObject element : elements) {
                                list.add(element);
                            }

                            list.sort(Comparator.comparing(o -> o.getLong("ts")));
                            for (int i = 0; i < list.size(); i++) {
                                if (i == 0) {
                                    list.get(i).getJSONObject("common").put("is_new", "1");
                                    firstVisitState.update(list.get(i).getLong("ts"));
                                } else {
                                    list.get(i).getJSONObject("common").put("is_new", "0");
                                }
                                out.collect(list.get(i));
                            }
                        } else {
                            // 2. 如果不是这个mid的首个窗口, 则所有均为非首次访问
                            elements.forEach(data -> {
                                data.getJSONObject("common").put("is_new", "0");
                                out.collect(data);
                            });
                        }
                    }
                });
        return result;
    }
}