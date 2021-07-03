package org.hxl.realtime.util;

/**
 * @author Grant
 * @create 2021-07-03 5:51
 */
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ThreadPoolUtil {
    public static ThreadPoolExecutor getThreadPool() {
        return new ThreadPoolExecutor(
                100,  // 线程池中允许同时运行的最大线程数
                300,  // 线程池中最大的线程数量
                300, // 最大空闲时间
                TimeUnit.SECONDS,
                new LinkedBlockingDeque<>(100)  // 阻塞队列中最多存储的请求数
        );
    }
}
