package org.hxl.gmallsuger.mapper;

/**
 * @author Grant
 * @create 2021-07-08 9:54
 */
import org.hxl.gmallsuger.bean.ProvinceStats;
import org.apache.ibatis.annotations.Select;

import java.util.List;


public interface ProvinceStatsMapper {
    @Select("SELECT\n" +
            "    province_name,\n" +
            "    sum(order_amount) AS order_amount,\n" +
            "    sum(order_count) AS order_count\n" +
            "FROM province_stats_2021\n" +
            "WHERE toYYYYMMDD(stt) = #{date}\n" +
            "GROUP BY province_name")
    List<ProvinceStats> getProvinceStatsByProvinceName(int date);

}
