package org.hxl.gmallsuger.mapper;

/**
 * @author Grant
 * @create 2021-07-08 9:52
 */
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;
import java.util.Map;

public interface KeywordStatsMapper {

    @Select("SELECT\n" +
            "    keyword,\n" +
            "    sum(ct * multiIf(source = 'search', 10, source = 'click', 8, source = 'order', 5, 2)) AS score\n" +
            "FROM keyword_stats_2021\n" +
            "WHERE toYYYYMMDD(stt) = #{date}\n" +
            "GROUP BY keyword\n" +
            "ORDER BY score DESC\n" +
            "LIMIT #{limit}")
    List<Map<String, Object>> statsKeyword(@Param("date") int date, @Param("limit") int limit);

}
