package org.hxl.realtime.function;

/**
 * @author Grant
 * @create 2021-07-03 5:50
 */
import org.hxl.realtime.common.Constant;
import org.hxl.realtime.util.JDBCUtil;
import org.hxl.realtime.util.RedisUtil;
import org.hxl.realtime.util.ThreadPoolUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.util.concurrent.ThreadPoolExecutor;

public abstract class DimAsyncFunction<T> extends RichAsyncFunction<T, T> {

    private ThreadPoolExecutor threadPool;
    private Connection conn;

    public abstract void addDim(Connection conn,
                                Jedis jedis,
                                T input,
                                ResultFuture<T> resultFuture) throws Exception;

    @Override
    public void open(Configuration parameters) throws Exception {
        threadPool = ThreadPoolUtil.getThreadPool();
        conn = JDBCUtil.getJdbcConnection(Constant.PHOENIX_DRIVER, Constant.PHOENIX_URL);

    }

    @Override
    public void close() throws Exception {
        if (threadPool != null) {
            threadPool.shutdown();
        }
        if (conn != null) {
            conn.close();
        }
    }

    // 异步调用, 每碰到一条数据, Flink会自动的调用这个方法, 可以把我们的业务处理逻辑放在这里
    @Override
    public void asyncInvoke(T input,  // 输入的数据
                            ResultFuture<T> resultFuture) throws Exception {
        // Phoenix客户端目前不支持异步方式进行读, 所以我们需要自己使用线程来完成这个异步的请求
        // 把input的补齐维度之后, 就可以把他交给 resultFuture, 这个数据就会进入流的下个阶段
        threadPool.execute(new Runnable() {
            @Override
            public void run() {
                // dim查询工作
                // redis客户端使用的连接池, 应该一个异步请求使用一个客户端, 否则会出现问题
                Jedis jedis = RedisUtil.getRedisClient();
                // 具体的维度补充的业务, 这个业务是比较抽象的, 和使用者有关系
                try {
                    addDim(conn, jedis, input, resultFuture);
                } catch (Exception e) {
                    throw new RuntimeException("异步读取维度数据出错....");
                }

                jedis.close();
            }
        });
    }
}
