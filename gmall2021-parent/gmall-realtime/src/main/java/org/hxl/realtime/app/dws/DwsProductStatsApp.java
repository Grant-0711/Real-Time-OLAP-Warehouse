package org.hxl.realtime.app.dws;

/**
 * @author Grant
 * @create 2021-07-05 4:17
 */

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.hxl.realtime.app.BaseAppV2;
import org.hxl.realtime.bean.OrderWide;
import org.hxl.realtime.bean.PaymentWide;
import org.hxl.realtime.bean.ProductStats;
import org.hxl.realtime.function.DimAsyncFunction;
import org.hxl.realtime.util.CommonUtil;
import org.hxl.realtime.util.DimUtil;
import org.hxl.realtime.util.FlinkSinkUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

import static org.hxl.realtime.common.Constant.*;

public class DwsProductStatsApp extends BaseAppV2 {
    public static void main(String[] args) {
        new DwsProductStatsApp().init(4002, 1, "DwsProductStatsApp", "DwsProductStatsApp",
                TOPIC_DWD_PAGE_LOG, TOPIC_DWD_DISPLAY_LOG,
                TOPIC_DWD_FAVOR_INFO, TOPIC_DWD_CART_INFO,
                TOPIC_DWM_ORDER_WIDE, TOPIC_DWM_PAYMENT_WIDE,
                TOPIC_DWD_ORDER_REFUND_INFO, TOPIC_DWD_COMMENT_INFO);
    }

    @Override
    public void run(StreamExecutionEnvironment env,
                    HashMap<String, DataStreamSource<String>> dsMap) {
        // 1. 8个流union到一起
        DataStream<ProductStats> productStatsStream = parseStreamAndUnion(dsMap);
        // 2. 开窗就集合
        SingleOutputStreamOperator<ProductStats> aggregatedStream = aggregateByWindowAndSkuId(productStatsStream);
        // 3. 补充维度信息
        SingleOutputStreamOperator<ProductStats> aggregatedStreamWithDim = joinDim(aggregatedStream);
        // 4. 把数据写入到clickhouse中
        sendToClickhouse(aggregatedStreamWithDim);

    }

    private void sendToClickhouse(SingleOutputStreamOperator<ProductStats> stream) {
        stream.addSink(FlinkSinkUtil.getClickhouseSink("gmall2021", "product_stats_2021", ProductStats.class));
    }

    private SingleOutputStreamOperator<ProductStats> joinDim(SingleOutputStreamOperator<ProductStats> aggregatedStream) {
        return AsyncDataStream.unorderedWait(
                aggregatedStream,
                new DimAsyncFunction<ProductStats>() {
                    @Override
                    public void addDim(Connection conn,
                                       Jedis jedis,
                                       ProductStats input,
                                       ResultFuture<ProductStats> resultFuture) throws Exception {
                        // 1. 补齐sku信息, 和 spu_id, tm_id c3_id
                        JSONObject skuInfo = DimUtil.readDim(conn, jedis, DIM_SKU_INFO, input.getSku_id());
                        input.setSku_name(skuInfo.getString("SKU_NAME"));
                        input.setSku_price(skuInfo.getBigDecimal("PRICE"));

                        input.setSpu_id(skuInfo.getLong("SPU_ID"));
                        input.setTm_id(skuInfo.getLong("TM_ID"));
                        input.setCategory3_id(skuInfo.getLong("CATEGORY3_ID"));

                        // 2. 补齐spu信息
                        JSONObject spuInfo = DimUtil.readDim(conn, jedis, DIM_SPU_INFO, input.getSpu_id());
                        input.setSpu_name(spuInfo.getString("SPU_NAME"));
                        // 3. 补齐tm信息
                        JSONObject tmInfo = DimUtil.readDim(conn, jedis, DIM_BASE_TRADEMARK, input.getTm_id());
                        input.setTm_name(tmInfo.getString("TM_NAME"));
                        // 3. 补齐c3信息
                        JSONObject c3Info = DimUtil.readDim(conn, jedis, DIM_BASE_CATEGORY3, input.getCategory3_id());
                        input.setCategory3_name(c3Info.getString("NAME"));

                        resultFuture.complete(Collections.singletonList(input));
                    }
                },
                300,
                TimeUnit.SECONDS);
    }

    private SingleOutputStreamOperator<ProductStats> aggregateByWindowAndSkuId(DataStream<ProductStats> productStatsStream) {
        SingleOutputStreamOperator<ProductStats> aggregatedStream = productStatsStream
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<ProductStats>forBoundedOutOfOrderness(Duration.ofSeconds(20))
                                .withTimestampAssigner((ps, ts) -> ps.getTs())
                )
                .keyBy(ProductStats::getSku_id)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .sideOutputLateData(new OutputTag<ProductStats>("late") {})
                .reduce(
                        new ReduceFunction<ProductStats>() {
                            @Override
                            public ProductStats reduce(ProductStats ps1,
                                                       ProductStats ps2) throws Exception {

                                ps1.setDisplay_ct(ps1.getDisplay_ct() + ps2.getDisplay_ct());
                                ps1.setClick_ct(ps1.getClick_ct() + ps2.getClick_ct());
                                ps1.setFavor_ct(ps1.getFavor_ct() + ps2.getFavor_ct());
                                ps1.setCart_ct(ps1.getCart_ct() + ps2.getCart_ct());

                                ps1.setOrder_amount(ps1.getOrder_amount().add(ps2.getOrder_amount()));
                                ps1.setOrder_sku_num(ps1.getOrder_sku_num() + ps2.getOrder_sku_num());
                                ps1.getOrderIdSet().addAll(ps2.getOrderIdSet());

                                ps1.setPayment_amount(ps1.getPayment_amount().add(ps2.getPayment_amount()));
                                ps1.getPaidOrderIdSet().addAll(ps2.getPaidOrderIdSet());

                                ps1.setRefund_amount(ps1.getRefund_amount().add(ps2.getRefund_amount()));
                                ps1.getRefundOrderIdSet().addAll(ps2.getRefundOrderIdSet());

                                ps1.setComment_ct(ps1.getComment_ct() + ps2.getComment_ct());
                                ps1.setGood_comment_ct(ps1.getGood_comment_ct() + ps2.getGood_comment_ct());

                                return ps1;
                            }
                        },
                        new ProcessWindowFunction<ProductStats, ProductStats, Long, TimeWindow>() {
                            @Override
                            public void process(Long key,
                                                Context context,
                                                Iterable<ProductStats> elements,
                                                Collector<ProductStats> out) throws Exception {
                                TimeWindow w = context.window();
                                ProductStats ps = elements.iterator().next();
                                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                                ps.setStt(sdf.format(w.getStart()));
                                ps.setEdt(sdf.format(w.getEnd()));

                                // 和订单相关的3个count进行计算
                                ps.setOrder_ct((long)ps.getOrderIdSet().size());
                                ps.setPaid_order_ct((long)ps.getPaidOrderIdSet().size());
                                ps.setRefund_order_ct((long)ps.getRefundOrderIdSet().size());

                                out.collect(ps);
                            }
                        }
                );

       /* aggregatedStream.print("正常");
        aggregatedStream.getSideOutput(new OutputTag<ProductStats>("late") {}).print("late");*/
        return aggregatedStream;
    }

    private DataStream<ProductStats> parseStreamAndUnion(HashMap<String, DataStreamSource<String>> dsMap) {

        // 1. 获取点击量
        SingleOutputStreamOperator<ProductStats> productClickStatsStream = dsMap
                .get(TOPIC_DWD_PAGE_LOG)
                .process(new ProcessFunction<String, ProductStats>() {
                    @Override
                    public void processElement(String jsonStr,
                                               Context ctx,
                                               Collector<ProductStats> out) throws Exception {
                        JSONObject obj = JSON.parseObject(jsonStr);
                        JSONObject page = obj.getJSONObject("page");
                        String pageId = page.getString("page_id");
                        // 只有pageId是 good_detail表示用户点击了某个商品进入了这个详情
                        if ("good_detail".equals(pageId)) {
                            ProductStats ps = new ProductStats();
                            ps.setSku_id(page.getLong("item"));
                            ps.setTs(obj.getLong("ts"));
                            ps.setClick_ct(1L);
                            out.collect(ps);
                        }
                    }
                });

        // 2. 获取产品曝光量
        SingleOutputStreamOperator<ProductStats> productDisplayStatsStream = dsMap
                .get(TOPIC_DWD_DISPLAY_LOG)
                .process(new ProcessFunction<String, ProductStats>() {
                    @Override
                    public void processElement(String value,
                                               Context ctx,
                                               Collector<ProductStats> out) throws Exception {
                        JSONObject obj = JSON.parseObject(value);
                        String itemType = obj.getString("item_type");
                        if ("sku_id".equals(itemType)) {
                            ProductStats ps = new ProductStats();
                            ps.setSku_id(obj.getLong("item"));
                            ps.setTs(obj.getLong("ts"));
                            ps.setDisplay_ct(1L);
                            out.collect(ps);
                        }
                    }
                });

        // 3. 获取收藏流
        SingleOutputStreamOperator<ProductStats> productFavorStatsStream = dsMap
                .get(TOPIC_DWD_FAVOR_INFO)
                .map(jsonStr -> {
                    JSONObject obj = JSON.parseObject(jsonStr);
                    ProductStats ps = new ProductStats();
                    ps.setSku_id(obj.getLong("sku_id"));
                    ps.setTs(CommonUtil.toTs(obj.getString("create_time")));
                    ps.setFavor_ct(1L);
                    return ps;

                });
        // 4. 获取购物车
        SingleOutputStreamOperator<ProductStats> productCartStatsStream = dsMap
                .get(TOPIC_DWD_CART_INFO)
                .map(jsonStr -> {
                    JSONObject obj = JSON.parseObject(jsonStr);
                    ProductStats ps = new ProductStats();
                    ps.setSku_id(obj.getLong("sku_id"));
                    ps.setTs(CommonUtil.toTs(obj.getString("create_time")));
                    ps.setCart_ct(1L);
                    return ps;

                });

        // 5. 下单
        SingleOutputStreamOperator<ProductStats> productOrderStatsStream = dsMap
                .get(TOPIC_DWM_ORDER_WIDE)
                .map(jsonStr -> {
                    OrderWide orderWide = JSON.parseObject(jsonStr, OrderWide.class);

                    ProductStats ps = new ProductStats();
                    ps.setSku_id(orderWide.getSku_id());
                    ps.setTs(CommonUtil.toTs(orderWide.getCreate_time()));
                    ps.setOrder_amount(orderWide.getSplit_total_amount());  // 这个商品在这个订单里占的金额
                    ps.setOrder_sku_num(orderWide.getSku_num());

                    // 等最后聚合的时候获取set的长度即可
                    ps.getOrderIdSet().add(orderWide.getOrder_id()); // 这个用来统计这个商品被多个订单下过
                    return ps;
                });
        // 6. 支付
        SingleOutputStreamOperator<ProductStats> productPaymentStatsStream = dsMap
                .get(TOPIC_DWM_PAYMENT_WIDE)
                .map(jsonStr -> {
                    PaymentWide paymentWide = JSON.parseObject(jsonStr, PaymentWide.class);
                    ProductStats ps = new ProductStats();
                    ps.setSku_id(paymentWide.getSku_id());
                    ps.setTs(CommonUtil.toTs(paymentWide.getPayment_create_time()));
                    ps.setPayment_amount(paymentWide.getSplit_total_amount());

                    ps.getPaidOrderIdSet().add(paymentWide.getOrder_id());
                    return ps;
                });

        // 7. 退款
        SingleOutputStreamOperator<ProductStats> productRefundStatsStream = dsMap
                .get(TOPIC_DWD_ORDER_REFUND_INFO)
                .map(jsonStr -> {
                    JSONObject obj = JSON.parseObject(jsonStr);

                    ProductStats ps = new ProductStats();
                    ps.setSku_id(obj.getLong("sku_id"));
                    ps.setTs(CommonUtil.toTs(obj.getString("create_time")));
                    ps.setRefund_amount(obj.getBigDecimal("refund_amount"));

                    ps.getRefundOrderIdSet().add(obj.getLong("order_id"));
                    return ps;
                });

        // 8. 评论
        SingleOutputStreamOperator<ProductStats> productCommentStatsStream = dsMap
                .get(TOPIC_DWD_COMMENT_INFO)
                .map(jsonStr -> {
                    JSONObject obj = JSON.parseObject(jsonStr);

                    ProductStats ps = new ProductStats();
                    ps.setSku_id(obj.getLong("sku_id"));
                    ps.setTs(CommonUtil.toTs(obj.getString("create_time")));

                    ps.setComment_ct(1L);

                    if (FIVE_START_GOOD_COMMENT.equals(obj.getString("appraise"))
                            || FOUR_START_GOOD_COMMENT.equals(obj.getString("appraise"))) {
                        ps.setGood_comment_ct(1L);
                    }
                    return ps;
                });

        return productClickStatsStream.union(
                productDisplayStatsStream,
                productFavorStatsStream,
                productCartStatsStream,
                productOrderStatsStream,
                productPaymentStatsStream,
                productRefundStatsStream,
                productCommentStatsStream);
    }

}
