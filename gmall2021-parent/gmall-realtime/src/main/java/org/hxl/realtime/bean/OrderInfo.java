package org.hxl.realtime.bean;

/**
 * @author Grant
 * @create 2021-07-02 4:22
 */
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
@Data
@AllArgsConstructor
@NoArgsConstructor

public class OrderInfo {
    private Long id;
    private Long province_id;
    private String order_status;
    private Long user_id;
    private BigDecimal total_amount;
    private BigDecimal activity_reduce_amount;
    private BigDecimal coupon_reduce_amount;
    private BigDecimal original_total_amount;
    private BigDecimal feight_fee;
    private String expire_time;
    private String create_time;
    private String operate_time;

    private String create_date; // 把其他字段处理得到
    private String create_hour;
    private Long create_ts;

    // 为了create_ts时间戳赋值, 所以需要手动补充
    public void setCreate_time(String create_time) throws ParseException {
        this.create_time = create_time;

        this.create_date = this.create_time.substring(0, 10); // 年月日
        this.create_hour = this.create_time.substring(11, 13); // 小时

        final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        this.create_ts = sdf.parse(create_time).getTime();
    }

}
