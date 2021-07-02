package org.hxl.realtime.sink;
import com.alibaba.fastjson.JSONObject;
import org.hxl.realtime.bean.TableProcess;
import org.hxl.realtime.common.Constant;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.hxl.realtime.util.RedisUtil;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author Grant
 * @create 2021-06-28 5:24
 */
public class PhoenixSink extends RichSinkFunction<Tuple2<JSONObject, TableProcess>> {

    private Connection conn;
    private ValueState<Boolean> createTableState;

    @Override
    public void open(Configuration parameters) throws Exception {
        // 0. 建立到Phoenix的连接. 使用标准的jdbc连接就可以了
        // 1. 加载驱动(常见的数据库可以不用加载, java会根据url自动的加载, 有些数据库比较加载, 比如: Phoenix)
        Class.forName(Constant.PHOENIX_DRIVER);
        // 2. 获取连接对象
        conn = DriverManager.getConnection(Constant.PHOENIX_URL);

        createTableState = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("createTableState", Boolean.class));
    }

    @Override
    public void invoke(Tuple2<JSONObject, TableProcess> value,
                       Context context) throws Exception {
        // 1. 检测表是否存在, 如果不存在需要先建表
        checkTable(value);
        // 2.  把这条数据写入到Phoenix中
        write2Hbase(value);

        // 3. dwm层需要的: 更新redis中的缓存
        // 如果检测到这次是update操作, 则需要去缓存中更新对应的维度数据或者删除对应的数据
        delCache(value);
    }

    private void delCache(Tuple2<JSONObject, TableProcess> value) {

        String key = value.f1.getSinkTable().toUpperCase() + ":" + value.f0.get("id");

        Jedis jedis = RedisUtil.getRedisClient();
        jedis.select(1);
        jedis.del(key);  // 如果key不存在, 会有啥影响?  没有影响. 所以不用提前判断key是否存在
        jedis.close();  // 归还给连接池
    }

    private void write2Hbase(Tuple2<JSONObject, TableProcess> value) throws SQLException {
        JSONObject data = value.f0;
        TableProcess tp = value.f1;
        // upsert into user(id, name, age) values(?,?,?)

        StringBuilder sql = new StringBuilder();
        String[] cs = tp.getSinkColumns().split(",");
        sql
                .append("upsert into ")
                .append(tp.getSinkTable())
                .append("(")
                .append(tp.getSinkColumns())
                .append(")values(");
        // 添加占位符 TODO
        // sql.append(tp.getSinkColumns().replaceAll("[^,]+", "?"));  // 正则替换
        for (String c : cs) {
            sql.append("?,");
        }
        sql.deleteCharAt(sql.length() - 1);

        sql.append(")");

        PreparedStatement ps = conn.prepareStatement(sql.toString());
        // 给占位符赋值
        for (int i = 0; i < cs.length; i++) {
            String v = data.get(cs[i]) == null ? "" : data.get(cs[i]).toString();  // 把所有的值变成string,然后就和Phoenix中的varchar对应了  "null" null
            ps.setString(i + 1, v);
        }
        ps.execute();
        conn.commit();
        ps.close();

    }

    // 检测表, 并创建不存在的表
    private void checkTable(Tuple2<JSONObject, TableProcess> value) throws SQLException, IOException {
        // 执行建表语句,实现在Phoenix中完成建表
        if (createTableState.value() == null) {  // 可以避免每来一条数据都需要去执行一次sql
            TableProcess tp = value.f1;
            // 1. 先拼接一个建表语句  TODO
            // create table if not exists t(name varchar, age varchar, constraint pk primary key(name, age))
            StringBuilder sql = new StringBuilder();
            sql
                    .append("create table if not exists ")
                    .append(tp.getSinkTable())
                    .append("(");
        /*for (String c : tp.getSinkColumns().split(",")) {
            sql.append(c).append(" varchar,");
        }
        sql.deleteCharAt(sql.length() - 1); // 去掉最后一个逗号*/
            sql
                    .append(tp.getSinkColumns().replaceAll(",", " varchar,"))
                    .append(" varchar, constraint pk primary key(");
            // 拼接主键
            sql.append(tp.getSinkPk() == null ? "id" : tp.getSinkPk());

            sql
                    .append("))")
                    .append(tp.getSinkExtend() == null ? "" : tp.getSinkExtend());

            // 2. 执行sql语句
            // 2.1 得到预处理语句
            System.out.println("建表语句: " + sql.toString());

            PreparedStatement ps = conn.prepareStatement(sql.toString());
            // 2.2 给sql中的占位符进行赋值 不需要给占位符赋值, 因为没有问号

            // 2.3 执行sql
            ps.execute();
            conn.commit();
            ps.close();

            createTableState.update(true);
        }

    }

    @Override
    public void close() throws Exception {
        // 是否资源

        if (conn != null) {
            conn.close();
        }
    }
}

