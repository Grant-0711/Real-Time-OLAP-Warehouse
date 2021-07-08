package org.hxl.gmallsuger.service;

/**
 * @author Grant
 * @create 2021-07-08 9:59
 */

import org.hxl.gmallsuger.bean.VisitorStats;
import org.hxl.gmallsuger.mapper.VisitorStatsMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/7/7 14:36
 */
@Service
public class VisitorServiceImpl implements VisitorStatsService {

    @Autowired
    VisitorStatsMapper visitor;

    @Override
    public List<VisitorStats> statsByHour(int date) {
        return visitor.statsByHour(date);
    }

    @Override
    public List<VisitorStats> statsByIsNew(int date) {
        return visitor.statsByIsNew(date);
    }

}
