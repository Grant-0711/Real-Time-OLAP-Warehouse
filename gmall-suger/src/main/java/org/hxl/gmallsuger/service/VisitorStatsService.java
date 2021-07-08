package org.hxl.gmallsuger.service;

/**
 * @author Grant
 * @create 2021-07-08 9:58
 */

import org.hxl.gmallsuger.bean.VisitorStats;

import java.util.List;


public interface VisitorStatsService {
    List<VisitorStats> statsByHour(int date);
    List<VisitorStats> statsByIsNew(int date);
}

