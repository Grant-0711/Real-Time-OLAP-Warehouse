package org.hxl.realtime.app.dwm;

/**
 * @author Grant
 * @create 2021-07-02 3:47
 */
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.hxl.realtime.app.BaseAppV1;
import org.hxl.realtime.common.Constant;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;
import org.apache.logging.log4j.util.Strings;

import java.time.Duration;
import java.util.List;
import java.util.Map;

public class DwmJumpDetailApp extends BaseAppV1 {
    public static void main(String[] args) {
        new DwmJumpDetailApp().init(3002,
                1,
                "DwmJumpDetailApp",
                "DwmJumpDetailApp",
                Constant.TOPIC_DWD_PAGE_LOG);
    }

    @Override
    public void run(StreamExecutionEnvironment env,
                    DataStreamSource<String> sourceStream) {
        // 使用cep
        sourceStream =
                env.fromElements(
                        "{\"common\":{\"mid\":\"101\"},\"page\":{\"page_id\":\"home\"},\"ts\":1000} ",
                        "{\"common\":{\"mid\":\"101\"},\"page\":{\"page_id\":\"home\"},\"ts\":2000} ",
                        "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"home\"},\"ts\":1000}",
                        "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"good_list\",\"last_page_id\":" +
                                "\"home\"},\"ts\":7000} ",
                        "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"good_list\",\"last_page_id\":" +
                                "\"detail\"},\"ts\":50000} "
                );


        // 1. 先有流
        KeyedStream<JSONObject, String> stream = sourceStream
                .map(JSON::parseObject)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((obj, ts) -> obj.getLong("ts"))
                )
                .keyBy(obj -> obj.getJSONObject("common").getString("mid"));

        // 2. 定义模式
        Pattern<JSONObject, JSONObject> pattern = Pattern
                .<JSONObject>begin("entry")
                .where(new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject value) throws Exception {
                        String lastPageId = value.getJSONObject("page").getString("last_page_id");
                        return lastPageId == null || lastPageId.length() == 0;
                    }
                })
                .next("second")
                .where(new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject value) throws Exception {
                        // 如果有入口, 后面又跟着其他的页面,这是正常访问.
                        JSONObject page = value.getJSONObject("page");
                        String pageId = page.getString("page_id");
                        String lastPageId = page.getString("last_page_id");
                    /*return (pageId != null && pageId.length() > 0
                        && lastPageId != null && lastPageId.length() > 0);*/
                        return Strings.isNotEmpty(pageId) && Strings.isNotEmpty(lastPageId);
                    }
                })
                .within(Time.seconds(5));
        // 3. 把模式应用到流上
        PatternStream<JSONObject> ps =  CEP.pattern(stream, pattern);
        // 4. 取出匹配到或者超时的数据

        SingleOutputStreamOperator<JSONObject> result = ps.select(
                new OutputTag<JSONObject>("jump") {},
                new PatternTimeoutFunction<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject timeout(Map<String, List<JSONObject>> pattern,
                                              long timeoutTimestamp) throws Exception {
                        return pattern.get("entry").get(0);  // 获取超时的数据, 就是我们需要的跳出明细
                    }

                },
                new PatternSelectFunction<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject select(Map<String, List<JSONObject>> pattern) throws Exception {
                        return null;  // 满足模式的正常数据, 对我们来说不需要, 可以直接返回null
                    }
                }
        );

        result.getSideOutput(new OutputTag<JSONObject>("jump") {}).print();

    }
}
