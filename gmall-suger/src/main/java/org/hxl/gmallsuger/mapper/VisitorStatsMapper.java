package org.hxl.gmallsuger.mapper;

/**
 * @author Grant
 * @create 2021-07-08 9:54
 */
import org.hxl.gmallsuger.bean.VisitorStats;
import org.apache.ibatis.annotations.Select;

import java.util.List;


public interface VisitorStatsMapper {

    @Select("SELECT\n" +
            "    toHour(stt) AS hour,\n" +
            "    sum(pv_ct) AS pv,\n" +
            "    sum(uv_ct) AS uv,\n" +
            "    sum(uj_ct) AS uj\n" +
            "FROM visitor_stats_2021\n" +
            "WHERE toYYYYMMDD(stt) = #{date}\n" +
            "GROUP BY toHour(stt)\n")
    List<VisitorStats> statsByHour(int date);


    @Select("SELECT\n" +
            "    is_new AS is_new,\n" +
            "    sum(pv_ct) AS pv,\n" +
            "    sum(uv_ct) AS uv,\n" +
            "    sum(uj_ct) AS uj\n" +
            "FROM visitor_stats_2021\n" +
            "WHERE toYYYYMMDD(stt) = #{date}\n" +
            "GROUP BY is_new\n")
    List<VisitorStats> statsByIsNew(int date);



}
