package org.hxl.gmallsuger.service;

/**
 * @author Grant
 * @create 2021-07-08 9:56
 */
import org.hxl.gmallsuger.bean.C3Stats;
import org.hxl.gmallsuger.bean.SpuStats;
import org.hxl.gmallsuger.bean.TmStats;

import java.math.BigDecimal;
import java.util.List;



public interface ProductStatsService {
    BigDecimal getGMV(int date);

    List<TmStats> getGMVByTM(int date, int limit);

    List<C3Stats> getGMVByC3(int date, int limit);

    List<SpuStats> getGMVBySpu(int date, int limit);

}
