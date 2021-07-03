package org.hxl.realtime.app.dwm;

/**
 * @author Grant
 * @create 2021-07-03 6:25
 */
import com.alibaba.fastjson.JSON;
import org.hxl.realtime.app.BaseAppV2;
import org.hxl.realtime.bean.OrderWide;
import org.hxl.realtime.bean.PaymentInfo;
import org.hxl.realtime.bean.PaymentWide;
import org.hxl.realtime.common.Constant;
import org.hxl.realtime.util.CommonUtil;
import org.hxl.realtime.util.FlinkSinkUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.HashMap;


import static org.hxl.realtime.common.Constant.*;

public class DwmPaymentWideApp extends BaseAppV2 {
    public static void main(String[] args) {
        new DwmPaymentWideApp().init(3004, 1, "DwmPaymentWideApp", "DwmPaymentWideApp",
                TOPIC_DWD_PAYMENT_INFO, TOPIC_DWM_ORDER_WIDE);
    }

    @Override
    public void run(StreamExecutionEnvironment env, HashMap<String, DataStreamSource<String>> dsMap) {
        KeyedStream<PaymentInfo, Long> paymentInfoStream = dsMap
                .get(TOPIC_DWD_PAYMENT_INFO)
                .map(json -> JSON.parseObject(json, PaymentInfo.class))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<PaymentInfo>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((pay, ts) -> CommonUtil.toTs(pay.getCreate_time()))
                )
                .keyBy(PaymentInfo::getOrder_id);

        KeyedStream<OrderWide, Long> orderWideStream = dsMap
                .get(TOPIC_DWM_ORDER_WIDE)
                .map(json -> JSON.parseObject(json, OrderWide.class))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<OrderWide>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((order, ts) -> CommonUtil.toTs(order.getCreate_time()))
                )
                .keyBy(OrderWide::getOrder_id);

        // 对这两个流做interval join
        paymentInfoStream
                .intervalJoin(orderWideStream)
                .between(Time.minutes(-45), Time.seconds(5))  // 由于支付要远远的晚于订单宽表
                .process(new ProcessJoinFunction<PaymentInfo, OrderWide, PaymentWide>() {
                    @Override
                    public void processElement(PaymentInfo left,
                                               OrderWide right,
                                               Context ctx,
                                               Collector<PaymentWide> out) throws Exception {
                        out.collect(new PaymentWide(left, right));
                    }
                })
                .map(JSON::toJSONString)
                .addSink(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWM_PAYMENT_WIDE));

    }
}
/*
dwd_payment_info和dwm_order_wide进行join
 */
