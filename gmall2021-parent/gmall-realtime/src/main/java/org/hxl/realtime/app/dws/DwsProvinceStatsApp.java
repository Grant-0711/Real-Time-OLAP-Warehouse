package org.hxl.realtime.app.dws;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.hxl.realtime.app.BaseSqlApp;
import org.hxl.realtime.bean.ProvinceStats;
import org.hxl.realtime.util.FlinkSinkUtil;

/**
 * @author Grant
 * @create 2021-07-05 4:36
 */
public class DwsProvinceStatsApp extends BaseSqlApp {
    public static void main(String[] args) {
        new DwsProvinceStatsApp().init(4003, 1, "DwsProvinceStatsApp");
    }

    @Override
    public void run(StreamTableEnvironment tEnv) {
        // 1. 建立一个动态表A, 与source关联(Kafka的topic)
        TableResult tableResult = tEnv.executeSql("create table order_wide(" +
                "   province_id bigint, " +
                "   province_name string, " +
                "   province_area_code string, " +
                "   province_iso_code string, " +
                "   province_3166_2_code string, " +
                "   order_id bigint, " +
                "   split_total_amount decimal(20, 2)," +
                "   create_time string, " +
                "   et as to_timestamp(create_time), " +
                "   watermark for et as et - interval '5' second " +
                ")with(" +
                "   'connector' = 'kafka'," +
                "   'properties.bootstrap.servers' = 'hadoop107:9092,hadoop108:9092,hadoop109:9092'," +
                "   'properties.group.id' = 'DwsProvinceStatsApp'," +
                "   'topic' = 'dwm_order_wide'," +
                "   'scan.startup.mode' = 'latest-offset'," +
                "   'format' = 'json'" +
                ")");
        //tEnv.sqlQuery("select * from order_wide").execute().print();
        //tableResult.print();
        // 2. 建立一个动态表B 与sink关联(clickhouse中的table)
        /*tEnv.executeSql("create table province_stats_2021 (\n" +
                            "   stt string,\n" +
                            "   edt string,\n" +
                            "   province_id  bigint,\n" +
                            "   province_name string,\n" +
                            "   area_code string ,\n" +
                            "   iso_code string,\n" +
                            "   iso_3166_2 string , \n" +
                            "   order_amount decimal(20, 2),\n" +
                            "   order_count bigint, \n" +
                            "   ts bigint, \n" +
                            "   primary key(stt,edt,province_id) not enforced" +
                            ")with(" +
                            "   'connector' = 'clickhouse'," +
                            "   'url' = 'clickhouse://hadoop162:8123'," +
                            "   'database-name' = 'gmall2021'," +
                            "   'table-name' = 'province_stats_2021', " +
                            "   'sink.batch-size' = '3'," +
                            "   'sink.flush-interval' = '1000', " +
                            "    'sink.max-retries' = '3'" +
                            ")");*/

        // 3. 在这个动态A表上执行连续查询
        Table sqlQuery = tEnv
                .sqlQuery("select " +
                        " date_format(tumble_start(et, interval '5' second), 'yyyy-MM-dd HH:mm:ss') stt, " +
                        " date_format(tumble_end(et, interval '5' second), 'yyyy-MM-dd HH:mm:ss') edt, " +
                        " province_id, " +
                        " province_name, " +
                        " province_area_code area_code, " +
                        " province_iso_code iso_code, " +
                        " province_3166_2_code iso_3166_2," +
                        " sum(split_total_amount) order_amount, " +
                        " count(distinct(order_id)) order_count, " +
                        " unix_timestamp() * 1000 ts " +
                        "from order_wide " +
                        "group by " +
                        " tumble(et, interval '5' second), " +
                        " province_id, " +
                        " province_name, " +
                        " province_area_code, " +
                        " province_iso_code, " +
                        " province_3166_2_code");

//         4. 把上面连续查询的结果写入到动态表B中
        //         由于连接器与flink版本不兼容, 需要把动态表的数据转成流之后再写入
    // tEnv.executeSql("insert into province_stats_2021 select * from " + sqlQuery);

     DataStream<Tuple2<Boolean, ProvinceStats>> tuple2DataStream = tEnv
              .toRetractStream(sqlQuery, ProvinceStats.class);
       //tuple2DataStream.print();
       tuple2DataStream

              .filter(t -> t.f0)  // 把撤回的过滤掉
                .map(t-> t.f1)

              .addSink(FlinkSinkUtil.getClickhouseSink("gmall2021", "province_stats_2021", ProvinceStats.class));


    }

}
