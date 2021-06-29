package org.hxl.realtime.util;

/**
 * @author Grant
 * @create 2021-06-29 13:53
 */
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.hxl.realtime.common.Constant;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.util.List;

public class DimUtil {

    // 从Phoenix读取维度数据
    public static JSONObject readDimFromPhoenix(Connection conn,
                                                String table,
                                                Object id) throws Exception {
        String sql = "select * from " + table + " where id=?";

        // 可以执行各种sql, 查询的结果应该会有多行
        List<JSONObject> list = JDBCUtil.queryList(conn, sql, new Object[]{id.toString()}, JSONObject.class);

        if (list != null && list.size() > 0) {
            return list.get(0);
        }
        return new JSONObject();

    }

    // 动缓存读取维度数据
    public static JSONObject readDimFromCache(Jedis client,
                                              String table,
                                              Object id) {

        String key = table + ":" + id;
        String jsonStr = client.get(key);

        JSONObject dim = null;
        if (jsonStr != null) {
            dim = JSON.parseObject(jsonStr);
            // 每读一个维度, 应该重新计算一个24小时的过期时间   更新过期时间
            client.expire(key, Constant.DIM_EXPIRE_SECOND);
        }
        return dim;
    }

    // 把维度数据存入缓存中
    private static void saveDimToCache(Jedis client,
                                       String table,
                                       Object id,
                                       JSONObject dim) {

        String key = table + ":" + id;
        String value = dim.toJSONString();

        client.setex(key, Constant.DIM_EXPIRE_SECOND, value);  // 把维度写入到redis中
    }

    public static JSONObject readDim(Connection conn,
                                     Jedis client,
                                     String table,
                                     Object id) throws Exception {
        client.select(1); // 把数据保存1号库

        // 首先去缓存中读取, 缓存没有在从hbase中读取
        JSONObject dim = readDimFromCache(client, table, id);
        if (dim == null) {  // 缓存中没有数据
            System.out.println("从 Phoenix  读取维度:" + table + "  " + id);
            dim = readDimFromPhoenix(conn, table, id);
            // 把这次读到的dim数据存储到缓存中. 否则缓存中永远没有数据
            saveDimToCache(client, table, id, dim);
        } else {
            System.out.println("从 缓存  读取维度: " + table + "  " + id);
        }

        return dim;
    }

    public static void main(String[] args) throws Exception {
        Connection conn = JDBCUtil.getJdbcConnection(Constant.PHOENIX_DRIVER, Constant.PHOENIX_URL);
        JSONObject object = readDimFromPhoenix(conn, "dim_user_info", 50000000);
        System.out.println(object);
    }
}
/*
维度存储到redis的时候, 使用什么 样的数据结构:
 string list set hash zset

 存储维度数据选择哪个?
    1. 读取方便
        根据表和id应该能够找到对应的dim信息
        字符串:
        key:  "dim_user_info:1"
        value: dim的json格式字符串

        缺点: key太多, 不方便管理
            优化: 可以把所有的维度存储到一个单独的数据库中, 不用默认的0号库


        hash:
            key             field  value
            dim_user_info     id1   dim的json格式字符串
    2. 可以单独给每个维度设置过期时间
        字符串:
            给每个key单独设置过期时间


         hash:
            无法给每个field单独设置过期时间



 */
