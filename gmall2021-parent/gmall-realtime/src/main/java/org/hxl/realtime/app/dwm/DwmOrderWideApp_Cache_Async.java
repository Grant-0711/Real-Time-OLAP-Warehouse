package org.hxl.realtime.app.dwm;

/**
 * @author Grant
 * @create 2021-07-03 5:48
 */
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.hxl.realtime.app.BaseAppV2;
import org.hxl.realtime.bean.OrderDetail;
import org.hxl.realtime.bean.OrderInfo;
import org.hxl.realtime.bean.OrderWide;
import org.hxl.realtime.common.Constant;
import org.hxl.realtime.function.DimAsyncFunction;
import org.hxl.realtime.util.DimUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;

import org.apache.flink.util.Collector;
import org.hxl.realtime.util.FlinkSinkUtil;
import redis.clients.jedis.Jedis;
import java.util.concurrent.TimeUnit;

import java.sql.Connection;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;

import static org.hxl.realtime.common.Constant.*;

public class DwmOrderWideApp_Cache_Async extends BaseAppV2 {
    public static void main(String[] args) {
        new DwmOrderWideApp_Cache_Async().init(30033, 1, "DwmOrderWideApp_Cache_Async", "DwmOrderWideApp_Cache_Async",
                TOPIC_DWD_ORDER_INFO, TOPIC_DWD_ORDER_DETAIL);
    }

    @Override
    public void run(StreamExecutionEnvironment env, HashMap<String, DataStreamSource<String>> streamMap) {
        // 1. 两个事实表进行join
        SingleOutputStreamOperator<OrderWide> orderWideStreamWithoutDims = factJoin(streamMap);

        // 2. join维度信息
        SingleOutputStreamOperator<OrderWide> orderWideWithDim = dimJoin(orderWideStreamWithoutDims);

        // 3. 把OrderWide数据写入到kafka中
        sendToKafka(orderWideWithDim);

    }

    private void sendToKafka(SingleOutputStreamOperator<OrderWide> orderWideWithDim) {
        orderWideWithDim
                .map(JSON::toJSONString)
                .addSink(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWM_ORDER_WIDE));
    }

    private SingleOutputStreamOperator<OrderWide> dimJoin(SingleOutputStreamOperator<OrderWide> orderWideStreamWithoutDims) {
        // 使用异步的方式进行join
        return AsyncDataStream
                .unorderedWait(
                        orderWideStreamWithoutDims,
                        new DimAsyncFunction<OrderWide>() {
                            @Override
                            public void addDim(Connection conn,
                                               Jedis jedis,
                                               OrderWide orderWide,
                                               ResultFuture<OrderWide> resultFuture) throws Exception {
                                // 1. 读取用户的维度信息
                                JSONObject userInfo = DimUtil.readDim(conn, jedis, Constant.DIM_USER_INFO, orderWide.getUser_id());
                                orderWide.setUser_gender(userInfo.getString("GENDER"));
                                orderWide.calcUserAge(userInfo.getString("BIRTHDAY"));
                                // 2. 读取省份的信息
                                JSONObject provinceInfo = DimUtil.readDim(conn, jedis, Constant.DIM_BASE_PROVINCE, orderWide.getProvince_id());
                                orderWide.setProvince_name(provinceInfo.getString("NAME"));
                                orderWide.setProvince_iso_code(provinceInfo.getString("ISO_CODE"));
                                orderWide.setProvince_area_code(provinceInfo.getString("AREA_CODE"));
                                orderWide.setProvince_3166_2_code(provinceInfo.getString("ISO_3166_2"));

                                // 3. 读取sku信息
                                JSONObject skuInfo = DimUtil.readDim(conn, jedis, Constant.DIM_SKU_INFO, orderWide.getSku_id());
                                orderWide.setSku_name(skuInfo.getString("SKU_NAME"));
                                orderWide.setOrder_price(skuInfo.getBigDecimal("PRICE"));
                                orderWide.setSpu_id(skuInfo.getLong("SPU_ID"));
                                orderWide.setTm_id(skuInfo.getLong("TM_ID"));
                                orderWide.setCategory3_id(skuInfo.getLong("CATEGORY3_ID"));

                                // 4. 读取tm信息
                                JSONObject tmInfo = DimUtil.readDim(conn, jedis, Constant.DIM_BASE_TRADEMARK, orderWide.getTm_id());
                                orderWide.setTm_name(tmInfo.getString("TM_NAME"));

                                // 5. 读取spu信息
                                JSONObject spuInfo = DimUtil.readDim(conn, jedis, Constant.DIM_SPU_INFO, orderWide.getSpu_id());
                                orderWide.setSpu_name(spuInfo.getString("SPU_NAME"));

                                // 6. 读取c3信息
                                JSONObject c3Info = DimUtil.readDim(conn, jedis, Constant.DIM_BASE_CATEGORY3, orderWide.getCategory3_id());
                                orderWide.setCategory3_name(c3Info.getString("NAME"));

                                // 把补齐维度数据的OrderWide交给resultFuture, 会自动后序的流中
                                resultFuture.complete(Collections.singletonList(orderWide));
                            }
                        },
                        120,
                        TimeUnit.SECONDS
                );

    }

    private SingleOutputStreamOperator<OrderWide> factJoin(HashMap<String, DataStreamSource<String>> streamMap) {
        KeyedStream<OrderInfo, Long> orderInfoStream = streamMap
                .get(TOPIC_DWD_ORDER_INFO)
                .map(json -> JSON.parseObject(json, OrderInfo.class))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<OrderInfo>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((info, ts) -> info.getCreate_ts())

                )
                .keyBy(OrderInfo::getId);

        KeyedStream<OrderDetail, Long> orderDetailStream = streamMap
                .get(TOPIC_DWD_ORDER_DETAIL)
                .map(json -> JSON.parseObject(json, OrderDetail.class))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<OrderDetail>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((info, ts) -> info.getCreate_ts())

                )
                .keyBy(OrderDetail::getOrder_id);

        // 对这两个流进行interval join
        return orderInfoStream
                .intervalJoin(orderDetailStream)
                .between(Time.minutes(-5), Time.minutes(5))
                // join 完成应该是返回一张宽表, 这张宽表目前维度只有一些id , 其实是缺少一些维度信息
                .process(new ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>() {
                    @Override
                    public void processElement(OrderInfo left,
                                               OrderDetail right,
                                               Context ctx,
                                               Collector<OrderWide> out) throws Exception {
                        out.collect(new OrderWide(left, right));
                    }
                });

    }

}
