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
import org.hxl.realtime.common.Constant;
import org.hxl.realtime.util.CommonUtil;
import org.hxl.realtime.util.FlinkSinkUtil;

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
    public static void main(String[] args) {
        new DwmUvApp().init(3001, 2, "DwmUvApp", "DwmUvApp", Constant.TOPIC_DWD_PAGE_LOG);
    }

    @Override
    public void run(StreamExecutionEnvironment env,
                    DataStreamSource<String> sourceStream) {
        // uv的计算:
        sourceStream
                .map(JSON::parseObject)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((obj, ts) -> obj.getLong("ts"))
                )
                .keyBy(obj -> obj.getJSONObject("common").getString("mid"))
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .process(new ProcessWindowFunction<JSONObject, JSONObject, String, TimeWindow>() {

                    private SimpleDateFormat df;
                    private ValueState<Long> firstVisitState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        firstVisitState = getRuntimeContext()
                                .getState(new ValueStateDescriptor<Long>("firstVisitState", Long.class));

                        df = new SimpleDateFormat("yyyy-MM-dd");
                    }

                    @Override
                    public void process(String key,
                                        Context ctx,
                                        Iterable<JSONObject> elements,
                                        Collector<JSONObject> out) throws Exception {
                        // 如果到了第二天, 则应该首先清空状态
                        // 今天
                        String today = df.format(ctx.window().getEnd());
                        String last = df.format(firstVisitState.value() == null ? 0L : firstVisitState.value());
                        if (!today.equals(last)) {  // 证明天发生了变化
                            firstVisitState.clear();
                        }

                        // 如果找到我们的第一条记录
                        // 找到每天的第一个窗口的时间戳最小的那个数据
                        if (firstVisitState.value() == null) {

                            List<JSONObject> list = CommonUtil.toList(elements);
                            //                        Collections.min(list, (o1, o2) -> o1.getLong("ts").compareTo(o2.getLong("ts")))
                            JSONObject first = Collections.min(list, Comparator.comparing(o -> o.getLong("ts")));
                            out.collect(first);
                            firstVisitState.update(first.getLong("ts"));  // 更新第一条纪律的时间戳到状态中
                        }
                    }
                })
                .map(JSON::toString)
                .addSink(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWM_UV));

    }
}
/*
这个uv不是计算最终的值, 而是把每个用户当前的第一条访问记录写入到DWM层(kafka)
 */
