package org.hxl.gmallsuger.service;

/**
 * @author Grant
 * @create 2021-07-08 9:58
 */
import org.hxl.gmallsuger.bean.ProvinceStats;
import org.hxl.gmallsuger.mapper.ProvinceStatsMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/7/7 9:25
 */
@Service
public class ProvinceStatsServiceImpl implements ProvinceStatsService {

    @Autowired
    ProvinceStatsMapper mapper;
    @Override
    public List<ProvinceStats> getProvinceStatsByProvinceName(int date) {
        return mapper.getProvinceStatsByProvinceName(date);
    }

}

