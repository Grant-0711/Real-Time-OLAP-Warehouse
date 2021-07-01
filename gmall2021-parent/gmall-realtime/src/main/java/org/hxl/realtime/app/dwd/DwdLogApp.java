package org.hxl.realtime.app.dwd;

/**
 * @author Grant
 * @create 2021-06-21 19:22
 */

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import java.util.Comparator;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import java.time.Duration;
import java.util.List;
import org.apache.flink.util.OutputTag;
import org.hxl.realtime.app.BaseAppV1;
import org.hxl.realtime.util.CommonUtil;
import org.hxl.realtime.util.FlinkSinkUtil;
import static org.hxl.realtime.common.Constant.*;

public class DwdLogApp extends BaseAppV1 {
    public static void main(String[] args) {
        new DwdLogApp().init(2001,
                2,
                "DwdLogApp",
                "DwdLogApp",
                TOPIC_ODS_LOG);

    }

    @Override
    public void run(StreamExecutionEnvironment env, DataStreamSource<String> sourceStream) {
        //本身客户端业务有新老用户的标识，但是不够准确，
        // 需要用实时计算再次确认(不涉及业务操作，只是单纯的做个状态确认)。
        SingleOutputStreamOperator<JSONObject> validatedStream =dinstinguishNewOrOld(sourceStream);
        //分流 不同的日志进入不同的流中
        Tuple3<SingleOutputStreamOperator<JSONObject>, DataStream<JSONObject>, DataStream<JSONObject>>
                threeSteams = splitStream(validatedStream);
        //把不同流中的数据sink到不同的topic中，就得到了DWD层数据
        sendToKafka(threeSteams);

        sourceStream.print();
    }


    private SingleOutputStreamOperator<JSONObject>
    dinstinguishNewOrOld(DataStreamSource<String> sourceStream) {
        return sourceStream.
                map(JSON::parseObject)
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((obj, ts) -> obj.getLong("ts")))
                .keyBy(obj -> obj.getJSONObject("common").getString("mid"))
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .process(new ProcessWindowFunction<JSONObject, JSONObject, String, TimeWindow>() {
                    private ValueState<Long> firstVisitState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        firstVisitState = getRuntimeContext()
                                .getState(new ValueStateDescriptor<>("firstVisitState", Long.class));
                    }

                    @Override
                    public void process(String key,
                                        Context context,
                                        Iterable<JSONObject> elements,
                                        Collector<JSONObject> out) throws Exception {
                        //判断是否为第一个窗口，是排序，不是的话所有数据的isnew为0
                        if (firstVisitState.value() == null) {
                            List<JSONObject> list = CommonUtil.toList(elements);
                            list.sort(Comparator.comparing(o -> o.getLong("ts")));  // 升序
                            for (int i = 0; i < list.size(); i++) {
                                JSONObject common = list.get(i).getJSONObject("common");
                                if (i == 0) {
                                    common.put("is_new", 1);
                                    // 更新状态
                                    firstVisitState.update(list.get(i).getLong("ts"));
                                } else {
                                    common.put("is_new", 0);
                                }
                                // 把每个数据放入collector
                                out.collect(list.get(i));

                            }

                        } else {
                            for (JSONObject obj : elements) {
                                JSONObject common = obj.getJSONObject("common");
                                common.put("is_new", 0);
                                out.collect(obj);
                            }
                        }
                    }
                });
    }

    private Tuple3<SingleOutputStreamOperator<JSONObject>, DataStream<JSONObject>, DataStream<JSONObject>>
    splitStream(SingleOutputStreamOperator<JSONObject> stream) {
        OutputTag<JSONObject> pageTag = new OutputTag<JSONObject>("page") {};
        OutputTag<JSONObject> displayTag = new OutputTag<JSONObject>("displayTag") {};
        // 启动日志: 主流   页面和曝光进入侧输出流
        SingleOutputStreamOperator<JSONObject> start = stream
                .process(new ProcessFunction<JSONObject, JSONObject>() {
                    @Override
                    public void processElement(JSONObject input,
                                               Context ctx,
                                               Collector<JSONObject> out) throws Exception {
                        JSONObject start = input.getJSONObject("start");
                        if (start != null) { // 启动日志
                            // 把启动日志写入到主流中
                            out.collect(input);
                        } else {

                            // 1. 如果是页面
                            JSONObject page = input.getJSONObject("page");
                            if (page != null) {
                                ctx.output(pageTag, input);
                            }
                            // 2. 如果曝光
                            JSONArray displays = input.getJSONArray("displays");
                            if (displays != null) {
                                for (int i = 0; i < displays.size(); i++) {
                                    JSONObject display = displays.getJSONObject(i);
                                    // 把一些其他信息插入到display中
                                    display.put("ts", input.getLong("ts"));
                                    display.put("page_id", input.getJSONObject("page").getString("page_id"));

                                    display.putAll(input.getJSONObject("common"));

                                    ctx.output(displayTag, display);
                                }
                            }
                        }
                    }
                });

        return Tuple3.of(start, start.getSideOutput(pageTag), start.getSideOutput(displayTag));

    }

    private void sendToKafka(Tuple3<SingleOutputStreamOperator<JSONObject>, DataStream<JSONObject>, DataStream<JSONObject>> threeSteams) {
        threeSteams.f0
                .map(JSONAware::toJSONString)
                .addSink(FlinkSinkUtil.getKafkaSink(TOPIC_DWD_START_LOG));

        threeSteams.f1
                .map(JSONAware::toJSONString)
                .addSink(FlinkSinkUtil.getKafkaSink(TOPIC_DWD_PAGE_LOG));

        threeSteams.f2
                .map(JSONAware::toJSONString)
                .addSink(FlinkSinkUtil.getKafkaSink(TOPIC_DWD_DISPLAY_LOG));


    }

}
