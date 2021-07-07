package org.hxl.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.logging.log4j.util.Strings;
import org.hxl.realtime.app.BaseAppV2;
import org.hxl.realtime.bean.VisitorStats;
import org.hxl.realtime.util.FlinkSinkUtil;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.HashMap;

import static org.hxl.realtime.common.Constant.*;
import static org.hxl.realtime.common.Constant.TOPIC_DWM_USER_JUMP_DETAIL;

/**
 * @author Grant
 * @create 2021-07-07 14:05
 */
public class test extends BaseAppV2 {
    public static void main(String[] args) {
        new test().init(6007, 1, "test1App", "test1App",
                TOPIC_DWD_PAGE_LOG, TOPIC_DWM_UV, TOPIC_DWM_USER_JUMP_DETAIL);
    }

    @Override
    public void run(StreamExecutionEnvironment env,
                    HashMap<String, DataStreamSource<String>> dsMap) {

        // 1. 解析流中的数据为同一个类型, 然后union
        DataStream<VisitorStats> statsStream = parseAndUnion(dsMap);

        // 2. 分组开窗聚合
        SingleOutputStreamOperator<VisitorStats> aggregatedStream = aggregateAndWindow(statsStream);
        // 3. 数据写入到clickhouse中
        aggregatedStream.print();
        sendToClickhouse(aggregatedStream);
    }

    // 把聚合后的数据写入到clickhouse中
    private void sendToClickhouse(SingleOutputStreamOperator<VisitorStats> aggregatedStream) {
        // 对jdbc sink 进一步封装, 得到一个好用的clickhouse sink
        aggregatedStream.addSink(FlinkSinkUtil.getClickhouseSink("gmall2021",
                "test",
                VisitorStats.class));
    }

    // 对union后的流开窗分组聚合
    private SingleOutputStreamOperator<VisitorStats> aggregateAndWindow(DataStream<VisitorStats> statsStream) {
        SingleOutputStreamOperator<VisitorStats> result = statsStream
                /*.assignTimestampsAndWatermarks(
                    WatermarkStrategy
                        .<VisitorStats>forBoundedOutOfOrderness(Duration.ofSeconds(20))
                        .withTimestampAssigner((vs, ts) -> vs.getTs())
                )*/
                .keyBy(vs -> vs.getVc() + "_" + vs.getCh() + "_" + vs.getAr() + "_" + vs.getIs_new())
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .sideOutputLateData(new OutputTag<VisitorStats>("late") {})
                .reduce(
                        new ReduceFunction<VisitorStats>() {
                            @Override
                            public VisitorStats reduce(VisitorStats vs1, VisitorStats vs2) throws Exception {

                                vs1.setPv_ct(vs1.getPv_ct() + vs2.getPv_ct());
                                vs1.setUv_ct(vs1.getUv_ct() + vs2.getUv_ct());
                                vs1.setSv_ct(vs1.getSv_ct() + vs2.getSv_ct());
                                vs1.setUj_ct(vs1.getUj_ct() + vs2.getUj_ct());
                                vs1.setDur_sum(vs1.getDur_sum() + vs2.getDur_sum());
                                return vs1;
                            }
                        },
                        new ProcessWindowFunction<VisitorStats, VisitorStats, String, TimeWindow>() {
                            @Override
                            public void process(String key,
                                                Context ctx,
                                                Iterable<VisitorStats> elements,  // 一定只有一个值
                                                Collector<VisitorStats> out) throws Exception {
                                VisitorStats vs = elements.iterator().next();
                                TimeWindow w = ctx.window();
                                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                                vs.setStt(sdf.format(w.getStart()));
                                vs.setEdt(sdf.format(w.getEnd()));

                                out.collect(vs);

                            }
                        }
                );

        return result;

    }

    private DataStream<VisitorStats> parseAndUnion(HashMap<String, DataStreamSource<String>> dsMap) {
        DataStreamSource<String> pageStream = dsMap.get(TOPIC_DWD_PAGE_LOG);
        DataStreamSource<String> uvStream = dsMap.get(TOPIC_DWM_UV);
        DataStreamSource<String> userJumpSteam = dsMap.get(TOPIC_DWM_USER_JUMP_DETAIL);

        // 1. 计算pv 和 持续访问时间
        SingleOutputStreamOperator<VisitorStats> pvAndDuringTimeStatsStream = pageStream
                .map(s -> {
                    JSONObject obj = JSON.parseObject(s);
                    JSONObject common = obj.getJSONObject("common");
                    JSONObject page = obj.getJSONObject("page");

                    return new VisitorStats(
                            "", "",
                            common.getString("vc"),
                            common.getString("ch"),
                            common.getString("ar"),
                            common.getString("is_new"),
                            0L, 1L, 0L, 0L, page.getLong("during_time"),
                            obj.getLong("ts")
                    );
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<VisitorStats>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((vs, ts) -> vs.getTs())
                );

        // 2. uv 计算
        SingleOutputStreamOperator<VisitorStats> uvStatsStream = uvStream
                .map(s -> {
                    JSONObject obj = JSON.parseObject(s);
                    JSONObject common = obj.getJSONObject("common");

                    return new VisitorStats(
                            "", "",
                            common.getString("vc"),
                            common.getString("ch"),
                            common.getString("ar"),
                            common.getString("is_new"),
                            1L, 0L, 0L, 0L, 0L,
                            obj.getLong("ts")
                    );
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<VisitorStats>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((vs, ts) -> vs.getTs())
                );

        // 3. 跳出次数
        SingleOutputStreamOperator<VisitorStats> ujStatsStream = userJumpSteam
                .map(s -> {
                    JSONObject obj = JSON.parseObject(s);
                    JSONObject common = obj.getJSONObject("common");

                    return new VisitorStats(
                            "", "",
                            common.getString("vc"),
                            common.getString("ch"),
                            common.getString("ar"),
                            common.getString("is_new"),
                            0L, 0L, 0L, 1L, 0L,
                            obj.getLong("ts")
                    );
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<VisitorStats>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((vs, ts) -> vs.getTs())
                );

        // 4. 进入次数的计算  来源topic?
        SingleOutputStreamOperator<VisitorStats> svStatsStream = pageStream
                .flatMap(new FlatMapFunction<String, VisitorStats>() {
                    @Override
                    public void flatMap(String s,
                                        Collector<VisitorStats> out) throws Exception {
                        JSONObject obj = JSON.parseObject(s);
                        JSONObject common = obj.getJSONObject("common");
                        JSONObject page = obj.getJSONObject("page");

                        String lastPageId = page.getString("last_page_id");
                        if (Strings.isEmpty(lastPageId)) {
                            VisitorStats vs = new VisitorStats(
                                    "", "",
                                    common.getString("vc"),
                                    common.getString("ch"),
                                    common.getString("ar"),
                                    common.getString("is_new"),
                                    0L, 0L, 1L, 0L, 0L,
                                    obj.getLong("ts")
                            );
                            out.collect(vs);
                        }

                    }
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<VisitorStats>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((vs, ts) -> vs.getTs())
                );

        return pvAndDuringTimeStatsStream.union(uvStatsStream, ujStatsStream, svStatsStream);
    }
}

