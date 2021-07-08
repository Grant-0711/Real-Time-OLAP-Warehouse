package org.hxl.gmallsuger.service;

/**
 * @author Grant
 * @create 2021-07-08 9:55
 */
import java.math.BigInteger;
import java.util.Map;


public interface KeywordStatsService {
    // "小米"->802, ....
    Map<String, BigInteger> statsKeyword(int date, int limit);
}
