package org.hxl.realtime.app.dws;

/**
 * @author Grant
 * @create 2021-07-07 0:34
 */
import org.hxl.realtime.app.BaseSqlApp;
import org.hxl.realtime.bean.KeywordStats;
import org.hxl.realtime.common.Constant;
import org.hxl.realtime.function.KeyWordUdtf;
import org.hxl.realtime.function.KwProduct;
import org.hxl.realtime.util.FlinkSinkUtil;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwsProductKeywordStatsApp extends BaseSqlApp {
    public static void main(String[] args) {
        new DwsProductKeywordStatsApp().init(4004, 1, "DwsProductKeywordStatsApp");
    }

    @Override
    public void run(StreamTableEnvironment tEnv) {
        // 与kafka的topic关联
        tEnv.executeSql("create table product_stats(" +
                " stt string, " +
                " edt string, " +
                " sku_name string, " +
                " click_ct bigint, " +
                " order_ct bigint, " +
                " cart_ct bigint " +
                ")with(" +
                "   'connector' = 'kafka'," +
                "   'properties.bootstrap.servers' = 'hadoop107:9092,hadoop108:9092,hadoop109:9092'," +
                "   'properties.group.id' = 'DwsProductKeywordStatsApp'," +
                "   'topic' = '" + Constant.TOPIC_DWS_PRODUCT_STATS + "'," +
                "   'scan.startup.mode' = 'latest-offset'," +
                "   'format' = 'json'" +
                ")");

        // 1. 过滤出数量至少一个不为0的记录
        Table t1 = tEnv.sqlQuery("select " +
                " * " +
                "from " +
                "product_stats " +
                "where click_ct > 0 or order_ct > 0 or cart_ct > 0");
        tEnv.createTemporaryView("t1", t1);

        // 2. 对关键词进行分词
        tEnv.createTemporaryFunction("ik_analyzer", KeyWordUdtf.class);
        Table t2 = tEnv.sqlQuery("select" +
                " stt, " +
                " edt, " +
                " word, " +
                " click_ct," +
                " order_ct, " +
                " cart_ct " +
                "from t1 " +
                "join lateral table(ik_analyzer(sku_name)) on true");
        tEnv.createTemporaryView("t2", t2);
        // 3. 要不要对t2中的数据做聚合?   需要! 聚合字段? stt edt word
        Table t3 = tEnv.sqlQuery("select " +
                "stt, edt, word," +
                "sum(click_ct) click_ct, " +
                "sum(order_ct) order_ct, " +
                "sum(cart_ct) cart_ct " +
                "from t2 " +
                "group by stt, edt, word");
        tEnv.createTemporaryView("t3", t3);

        // 4. 开始把多列变多行
        tEnv.createTemporaryFunction("kw_product", KwProduct.class);

        Table table = tEnv.sqlQuery("select" +
                " stt, " +
                " edt, " +
                " word keyword," +
                " source," +
                " ct," +
                " unix_timestamp() * 1000 ts " +
                "from t3," +
                "lateral table(kw_product(click_ct, order_ct, cart_ct))");
        // 5. 把数据写入到ClickHouse中
        tEnv
                .toRetractStream(table, KeywordStats.class)
                .filter(t -> t.f0)
                .map(t -> t.f1)
                .addSink(FlinkSinkUtil.getClickhouseSink("gmall2021", "keyword_stats_2021", KeywordStats.class));

    }
}
/*
select   from a join b on a.id=b.id
select .. from a, b where a.id=b.id
CREATE TABLE keyword_stats_2021
(
    `stt` DateTime,
    `edt` DateTime,
    `keyword` String,
    `source` String,
    `ct` UInt64,
    `ts` UInt64
)


小米手机   10   20    40

华为手机   15   25    55

-----
变1:
小米   10   20    40
手机   10   20    40

华为   15   25    55
手机   15   25    55

----

变成:
 小米  'click' 10
 小米  'order' 20
 小米  'cart'  40
 手机  'click' 10
 手机  'order' 20
 手机  'cart' 40

 手机  ...... 15   25    55

 ----




 */
