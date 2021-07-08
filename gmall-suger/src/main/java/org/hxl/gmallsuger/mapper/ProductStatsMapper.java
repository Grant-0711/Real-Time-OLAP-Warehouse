package org.hxl.gmallsuger.mapper;

/**
 * @author Grant
 * @create 2021-07-08 9:53
 */

import org.hxl.gmallsuger.bean.C3Stats;
import org.hxl.gmallsuger.bean.SpuStats;
import org.hxl.gmallsuger.bean.TmStats;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.math.BigDecimal;
import java.util.List;


public interface ProductStatsMapper {
    @Select("SELECT sum(order_amount)\n" +
            "FROM product_stats_2021\n" +
            "WHERE toYYYYMMDD(stt) = #{date}")
    BigDecimal getGMV(@Param("date") int date);


    @Select("SELECT\n" +
            "    tm_name,\n" +
            "    sum(order_amount) AS order_amount\n" +
            "FROM product_stats_2021\n" +
            "WHERE toYYYYMMDD(stt) = #{date}\n" +
            "GROUP BY tm_name\n" +
            "ORDER BY order_amount DESC\n" +
            "LIMIT #{limit}\n")
    List<TmStats> getGMVByTM(@Param("date") int date, @Param("limit") int limit);

    @Select("SELECT\n" +
            "    category3_name,\n" +
            "    sum(order_amount) AS order_amount\n" +
            "FROM product_stats_2021\n" +
            "WHERE toYYYYMMDD(stt) = #{date}\n" +
            "GROUP BY category3_name\n" +
            "ORDER BY order_amount DESC\n" +
            "LIMIT #{limit}\n")
    List<C3Stats> getGMVByC3(@Param("date") int date, @Param("limit") int limit);

    @Select("SELECT\n" +
            "    spu_name,\n" +
            "    sum(order_amount) AS order_amount\n" +
            "FROM product_stats_2021\n" +
            "WHERE toYYYYMMDD(stt) = #{date}\n" +
            "GROUP BY spu_name\n" +
            "ORDER BY order_amount DESC\n" +
            "LIMIT #{limit}\n")
    List<SpuStats> getGMVBySpu(@Param("date") int date, @Param("limit") int limit);
}
