package org.hxl.realtime.bean;

/**
 * @author Grant
 * @create 2021-07-02 4:25
 */
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/6/30 9:13
 */
@Data
@AllArgsConstructor
@NoArgsConstructor

public class PaymentInfo {
    private Long id;
    private Long order_id;
    private Long user_id;
    private BigDecimal total_amount;
    private String subject;
    private String payment_type;
    private String create_time;
    private String callback_time;
}
