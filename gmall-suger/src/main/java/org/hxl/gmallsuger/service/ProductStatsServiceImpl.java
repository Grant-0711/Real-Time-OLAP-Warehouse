package org.hxl.gmallsuger.service;

/**
 * @author Grant
 * @create 2021-07-08 9:56
 */
import org.hxl.gmallsuger.bean.C3Stats;
import org.hxl.gmallsuger.bean.SpuStats;
import org.hxl.gmallsuger.bean.TmStats;
import org.hxl.gmallsuger.mapper.ProductStatsMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.List;


@Service
public class ProductStatsServiceImpl implements ProductStatsService {
    // 从mapper层读取数据, 提供controller调用

    @Autowired
    ProductStatsMapper product;
    @Override
    public BigDecimal getGMV(int date) {
        return product.getGMV(date);
    }

    @Override
    public List<TmStats> getGMVByTM(int date, int limit) {
        return product.getGMVByTM(date, limit);
    }

    @Override
    public List<C3Stats> getGMVByC3(int date, int limit) {
        return product.getGMVByC3(date, limit);
    }

    @Override
    public List<SpuStats> getGMVBySpu(int date, int limit) {
        return product.getGMVBySpu(date, limit);
    }

}

