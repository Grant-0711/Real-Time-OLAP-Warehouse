package org.hxl.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.hxl.realtime.app.BaseAppV1;
import org.hxl.realtime.util.CommonUtil;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import static org.hxl.realtime.common.Constant.TOPIC_DWD_PAGE_LOG;

/**
 * @author Grant
 * @create 2021-06-29 2:50
 */
/*
* 不是计算最终值，而是把每个用户的当前第一条访问记录写入dwm层
* */
public class DwmUvApp extends BaseAppV1 {
    @Override
    public void run(StreamExecutionEnvironment env, DataStreamSource<String> sourceStream) {
            sourceStream
                    .map(JSON::parseObject)
                    .assignTimestampsAndWatermarks(
                            WatermarkStrategy.
                                    <JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                            .withTimestampAssigner((obj,ts) -> obj.getLong("ts"))
                    )
                    .keyBy((obj -> obj.getJSONObject("common").getString("mid")))
                    .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                    .process(new ProcessWindowFunction<JSONObject, JSONObject, String, TimeWindow>() {
                        private ValueState<Long> firstVisitState;
                        private SimpleDateFormat df;
                        @Override
                        public void open(Configuration parameters) {
                            firstVisitState = getRuntimeContext()
                                    .getState(new ValueStateDescriptor<>("firstVisitState", Long.class));

                             df = new SimpleDateFormat("yyyy-MM-dd");
                        }

                        @Override
                        public void process(String key, Context context, Iterable<JSONObject> elements, Collector<JSONObject> outer) throws Exception {
                            //如果到了第二天，则首先清空状态
                            //今天
                            String today = df.format(context.window().getEnd());
                            String last = df.format(firstVisitState.value() == null? 0L :firstVisitState);
                            if (!today.equals(last)){
                                firstVisitState.clear();
                            }


                            if (firstVisitState.value() == null){
                                List<JSONObject> list = CommonUtil.toList(elements);
                                JSONObject first = Collections.min(list, Comparator.comparing(o -> o.getLong("ts")));
                                outer.collect(first);
                                firstVisitState.update(first.getLong("ts"));
                            }
                        }
                    })
                    .print();



    }

    public static void main(String[] args) {
        new DwmUvApp().init(3001,2,"DwmUvApp","DwmUvApp", TOPIC_DWD_PAGE_LOG);
    }
}
