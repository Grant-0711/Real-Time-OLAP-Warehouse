package org.hxl.realtime.app.dwd;

/**
 * @author Grant
 * @create 2021-06-28 5:26
 */
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.hxl.realtime.app.BaseAppV1;
import org.hxl.realtime.bean.TableProcess;
import org.hxl.realtime.common.Constant;
import org.hxl.realtime.util.FlinkSinkUtil;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.Arrays;
import java.util.List;

public class DwdDbApp extends BaseAppV1 {
    public static void main(String[] args) {
        new DwdDbApp().init(2002, 2, "DwdDbApp", "DwdDbApp", Constant.TOPIC_ODS_DB);
    }

    @Override
    public void run(StreamExecutionEnvironment env, DataStreamSource<String> sourceStream) {
        // 1. 读取配置表的数据
        SingleOutputStreamOperator<TableProcess> tpStream = readTableProcess(env);
        // 2. sourceStream 就是数据表的数据 做一些 etl
        SingleOutputStreamOperator<JSONObject> etledDataStream = etlDataStream(sourceStream);
        // 3. 把这两个流connect到一起
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> dataTpStream = connectStreams(tpStream, etledDataStream);
        // 4. 进行动态分流
        Tuple2<SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>>, DataStream<Tuple2<JSONObject, TableProcess>>> kafkaAndHbaseStreams
                = dynamicStream(dataTpStream);
        // 5. 事实表数据写入到Kafka
        sendToKafka(kafkaAndHbaseStreams.f0);
        // 5. 维度表数据写入到Hbase
        kafkaAndHbaseStreams.f1.print();
        sendToHbase(kafkaAndHbaseStreams.f1);

    }

    private void sendToHbase(DataStream<Tuple2<JSONObject, TableProcess>> stream) {
        /* 使用Phoenix向hbase写入数据
        1. Phoenix中的表如何创建
            动态的建表
                什么时机去建表?
                    第一条数据过来的时候去建对应维度表

        2. 如果向Phoenix写入数据
               动态写入
                sql语句根据不同的表拼出不同的sql

         */
        stream
                .keyBy(t -> t.f1.getSinkTable())  // 按照sink_table进行keyBy提高写入的效率
                .addSink(FlinkSinkUtil.getHBaseSink());

    }

    private void sendToKafka(SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> stream) {
        stream
                .addSink(FlinkSinkUtil.getKafkaSink());
    }

    private Tuple2<SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>>, DataStream<Tuple2<JSONObject, TableProcess>>> dynamicStream(SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> dataTpStream) {
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> kafkaStream = dataTpStream
                .process(new ProcessFunction<Tuple2<JSONObject, TableProcess>, Tuple2<JSONObject, TableProcess>>() {
                    @Override
                    public void processElement(Tuple2<JSONObject, TableProcess> value,
                                               Context ctx,
                                               Collector<Tuple2<JSONObject, TableProcess>> out) throws Exception {

                        // 去kafka放入主流  去hbase的放入侧输出流
                        TableProcess tp = value.f1;

                        // 1. 根据配置表中sink_columns的值, 数据做一些过滤   100  50需要sink, 把不需要sink的列删除
                        JSONObject data = value.f0.getJSONObject("data");
                        filterColumns(data, tp);

                        // 写入到kafka或者hbase的数据应该只有data中的数据, 不应包含其他的一些元数据了
                        Tuple2<JSONObject, TableProcess> result = Tuple2.of(data, tp);
                        if (TableProcess.SINK_TYPE_KAFKA.equals(tp.getSinkType())) {
                            //                        out.collect(value);
                            out.collect(result);
                        } else if (TableProcess.SINK_TYPE_HBASE.equals(tp.getSinkType())) {
                            // 把到hbase的数据写入到测输出流
                            ctx.output(new OutputTag<Tuple2<JSONObject, TableProcess>>("hbase") {}, result);
                        }

                    }

                    private void filterColumns(JSONObject data, TableProcess tp) {
                        // id,user_id,sku_id,cart_price,sku_num,img_url,sku_name,is_checked,create_time,operate_time,is_ordered,order_time,source_type,source_id
                   /* Set<String> keys = data.keySet();
                    Iterator<String> it = keys.iterator();
                    while (it.hasNext()) {
                        // 如果需要删应该在这个地方删
                    }*/
                        List<String> cs = Arrays.asList(tp.getSinkColumns().split(","));
                        data.keySet().removeIf(key -> !cs.contains(key));
                    }
                });

        DataStream<Tuple2<JSONObject, TableProcess>> hbaseStream = kafkaStream.getSideOutput(new OutputTag<Tuple2<JSONObject, TableProcess>>("hbase") {});
        return Tuple2.of(kafkaStream, hbaseStream);

    }

    private SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> connectStreams(SingleOutputStreamOperator<TableProcess> tpStream,
                                                                                        SingleOutputStreamOperator<JSONObject> etledDataStream) {
        MapStateDescriptor<String, TableProcess> tpStateDesc = new MapStateDescriptor<>("tpState",
                String.class,
                TableProcess.class);
        // 1. 先把配置表的流做成广播流
        /*
         假设来了一条user相关的数据, 需要知道这条数据应该去哪?
         有谁来决定这条数据的sink?   表名+操作类型

         根据这个两个的组合(key), 应该能够找到一个唯一的 TableProcess 对象(value)
         如何使用这个广播状态:
         */
        BroadcastStream<TableProcess> tpBCStream = tpStream
                .broadcast(tpStateDesc);

        // 2. 让数据流和广播流进行connect
        return etledDataStream
                .connect(tpBCStream)
                .process(new BroadcastProcessFunction<JSONObject, TableProcess, Tuple2<JSONObject, TableProcess>>() {
                    @Override
                    public void processElement(JSONObject value,
                                               ReadOnlyContext ctx,
                                               Collector<Tuple2<JSONObject, TableProcess>> out) throws Exception {
                        //处理数据流
                        ReadOnlyBroadcastState<String, TableProcess> tpState = ctx.getBroadcastState(tpStateDesc);

                    /*
                    {
                        "database":"gmall2021",
                        "table":"order_info",
                        "type":"update",
                        "ts":1624610711,
                        "xid":19672,
                        "xoffset":4210,
                        "data":Object{...},
                        "old":{
                            "order_status":"1001",
                            "operate_time":null
                        }
                    }
                     */
                        // 如果旧的维度是通过bootstrap来实现采集到的, 则这个时候type的值是: bootstrap-insert
                        String key = value.getString("table") + ":" + value.getString("type").replaceAll("bootstrap-", "");
                        TableProcess tp = tpState.get(key);
                        // 如果配置表中没有这个表相关的配置,则这个表就不需要写入到dwd层
                        if (tp != null) {
                            out.collect(Tuple2.of(value, tp));
                        } else {
                            // 如果没有对应的tp, 表示现在没有对应个配置: 1. 真的没有配置, 2. 或者配置还没有来
                            // 考虑把数据先存入到一个集合中, 等一段时间之后, 再去单独的处理这个集合
                        }
                    }

                    @Override
                    public void processBroadcastElement(TableProcess tp,
                                                        Context ctx,
                                                        Collector<Tuple2<JSONObject, TableProcess>> out) throws Exception {
                        // 处理广播流中的数据

                        // 1. 获取广播状态
                        BroadcastState<String, TableProcess> tpState = ctx.getBroadcastState(tpStateDesc);
                        // 2. 把配置写入到广播状态中
                        // 2.1 得到key
                        String key = tp.getSourceTable() + ":" + tp.getOperateType();
                        tpState.put(key, tp);

                    }
                });

    }

    private SingleOutputStreamOperator<JSONObject> etlDataStream(DataStreamSource<String> sourceStream) {
        //去除一些脏数据
        return sourceStream
                .map(JSON::parseObject)
                .filter(obj ->
                        obj.getString("database") != null
                                && obj.getString("table") != null
                                && obj.getString("type") != null
                                && (obj.getString("type").contains("insert") || obj.get("type").equals("update"))
                                && obj.getString("data") != null
                                && obj.getString("data").length() > 2
                );
    }
    /*
    {
    "database":"gmall2021",
    "table":"order_detail_activity",
    "type":"insert",
    "ts":1624606100,
    "xid":341,
    "xoffset":4141,
    "data":{

    }
}
     */

    // 读取配置表的数据: 使用flink-sql
    private SingleOutputStreamOperator<TableProcess> readTableProcess(StreamExecutionEnvironment env) {
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // 创建一个动态表与mysql的配置进行关联
        tEnv.executeSql("CREATE TABLE `table_process` (\n" +
                "  `source_table` string,\n" +
                "  `operate_type` string ,\n" +
                "  `sink_type` string ,\n" +
                "  `sink_table` string ,\n" +
                "  `sink_columns` string ,\n" +
                "  `sink_pk` string ,\n" +
                "  `sink_extend` string ,\n" +
                "  PRIMARY KEY (`source_table`,`operate_type`) not enforced" +
                ")with(" +
                " 'connector' = 'mysql-cdc',\n" +
                " 'hostname' = 'hadoop107',\n" +
                " 'port' = '3306',\n" +
                " 'username' = 'root',\n" +
                " 'password' = '123456',\n" +
                " 'database-name' = 'gmall2021_realtime',\n" +
                " 'table-name' = 'table_.*', " +
                " 'debezium.snapshot.mode'='initial' " +
                ")");

        Table tpTable = tEnv
                .sqlQuery("select " +
                        " source_table sourceTable, " +
                        " sink_type sinkType, " +
                        " operate_type operateType, " +
                        " sink_table sinkTable, " +
                        " sink_columns sinkColumns," +
                        " sink_pk sinkPk, " +
                        " sink_extend sinkExtend " +
                        "from table_process");

        return tEnv
                .toRetractStream(tpTable, TableProcess.class)
                .filter(t -> t.f0)
                .map(t -> t.f1);

    }
}
// 读取mysql的模式问题:
/*
snapshot.mode
    用来控制debezium 如何从mysql中去读数据

    initial  程序一启动, 则首先把表中所有的数据读取出来(通过sql查询)
                然后基于binlog来监控新增和变化

    never
        只通过binlog来监控新增和变化


采集数据的时候的读锁问题:
maxwell或者 debezium 都有一个功能, 读取全部的旧数据的功能



 */
