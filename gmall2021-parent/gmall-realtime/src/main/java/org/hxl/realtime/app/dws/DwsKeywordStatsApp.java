package org.hxl.realtime.app.dws;

/**
 * @author Grant
 * @create 2021-07-05 18:31
 */
import org.hxl.realtime.app.BaseSqlApp;
import org.hxl.realtime.bean.KeywordStats;
import org.hxl.realtime.common.Constant;
import org.hxl.realtime.function.KeyWordUdtf;
import org.hxl.realtime.util.FlinkSinkUtil;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwsKeywordStatsApp extends BaseSqlApp {
    public static void main(String[] args) {
        new DwsKeywordStatsApp().init(4003, 1, "DwsKeywordStatsApp");
    }

    @Override
    public void run(StreamTableEnvironment tEnv) {
        /*
        {
            "common":{
                "ar":"310000",
                "uid":"40",
                "os":"iOS 13.2.3",
                "ch":"Appstore",
                "is_new":"0",
                "md":"iPhone Xs Max",
                "mid":"mid_2",
                "vc":"v2.0.1",
                "ba":"iPhone"
            },
            "page":{
                "page_id":"",
                "during_time":18411,
                "last_page_id":""
            },
            "ts":1614602604000
        }
         */
        // 1. 建立一个动态表A, 与source关联(Kafka的topic)
        tEnv.executeSql("create table page_log(" +
                " common map<string, string>, " +
                " page map<string, string>, " +
                " ts bigint, " +
                " et as to_timestamp(from_unixtime(ts/1000)), " + // 先把long转成yyyy-MM-dd HH:mm:ss , 然后再转成时间戳
                " watermark for  et as et - interval '3' second" +
                ")with(" +
                "   'connector' = 'kafka'," +
                "   'properties.bootstrap.servers' = 'hadoop107:9092,hadoop107:9092,hadoop107:9092'," +
                "   'properties.group.id' = 'DwsKeywordStatsApp'," +
                "   'topic' = '" + Constant.TOPIC_DWD_PAGE_LOG + "'," +
                "   'scan.startup.mode' = 'latest-offset'," +
                "   'format' = 'json'" +
                ")");

        // 计算每个次的热度(每个次的搜索次数)
        // 1. 查出来用户搜索的词
        Table t1 = tEnv.sqlQuery("select" +
                " page['item'] kw, " +
                " et " +
                "from page_log " +
                "where page['item'] is not null " +
                "and page['page_id']='good_list'");
        tEnv.createTemporaryView("t1", t1);

        // 2. 对搜索的关键词进行分词
        // 2.1 注册自定义函数
        tEnv.createTemporaryFunction("ik_analyzer", KeyWordUdtf.class);

        // 2.2 使用自定义函数进行切词
        Table t2 = tEnv.sqlQuery("select" +
                " kw, " +
                " et, " +
                " word " +
                " from t1 " +
                " join lateral table(ik_analyzer(kw)) on true");
        tEnv.createTemporaryView("t2", t2);

        // 3. 开窗聚合每个word的次数
        /*
        CREATE TABLE keyword_stats_2021
        (
            `stt` DateTime,
            `edt` DateTime,
            `keyword` String,
            `source` String,
            `ct` UInt64,
            `ts` UInt64
        )
         */
        Table resultTable = tEnv
                .sqlQuery("select" +
                        " date_format(tumble_start(et, interval '10' second), 'yyyy-MM-dd HH:mm:ss') stt, " +
                        " date_format(tumble_end(et, interval '10' second), 'yyyy-MM-dd HH:mm:ss') edt, " +
                        " word keyword, " +
                        " 'search' source, " +
                        " count(*) ct, " +
                        " unix_timestamp() * 1000 ts " +
                        "from t2 " +
                        "group by word, tumble(et, interval '10' second)");

        tEnv
                .toRetractStream(resultTable, KeywordStats.class)
                .filter(t -> t.f0)
                .map(t -> t.f1)
                .addSink(FlinkSinkUtil.getClickhouseSink("gmall2021", "keyword_stats_2021", KeywordStats.class));

    }
}
/*
小米手机
华为手机
苹果手机
oppo手机

----
自定义函数:
    1. 标量函数
    2. 表值函数
    3. 聚合函数
    4. AggregateTable 聚合表值函数

 使用: 表值函数 table


小米
手机
华为
手机
苹果
手机
oppo
手机
---
再统计每个词的搜索次数



 */
