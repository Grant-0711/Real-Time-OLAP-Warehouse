package org.hxl.gmallsuger.service;

/**
 * @author Grant
 * @create 2021-07-08 9:57
 */
import org.hxl.gmallsuger.bean.ProvinceStats;

import java.util.List;


public interface ProvinceStatsService {
    List<ProvinceStats> getProvinceStatsByProvinceName(int date);

}
