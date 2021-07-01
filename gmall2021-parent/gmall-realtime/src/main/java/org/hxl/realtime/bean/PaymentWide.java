package org.hxl.realtime.bean;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.beanutils.BeanUtils;

import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;

/**
 * @author Grant
 * @create 2021-07-02 4:25
 */
@Data
@AllArgsConstructor
@NoArgsConstructor

public class PaymentWide {

    private Long payment_id;
    private String subject;
    private String payment_type;
    private String payment_create_time;
    private String callback_time;
    private Long detail_id;
    private Long order_id;
    private Long sku_id;
    private BigDecimal order_price;
    private Long sku_num;
    private String sku_name;
    private Long province_id;
    private String order_status;
    private Long user_id;
    private BigDecimal total_amount;
    private BigDecimal activity_reduce_amount;
    private BigDecimal coupon_reduce_amount;
    private BigDecimal original_total_amount;
    private BigDecimal feight_fee;
    private BigDecimal split_feight_fee;
    private BigDecimal split_activity_amount;
    private BigDecimal split_coupon_amount;
    private BigDecimal split_total_amount;
    private String order_create_time;

    private String province_name;//查询维表得到
    private String province_area_code;
    private String province_iso_code;
    private String province_3166_2_code;
    private Integer user_age;
    private String user_gender;

    private Long spu_id;     //作为维度数据 要关联进来
    private Long tm_id;
    private Long category3_id;
    private String spu_name;
    private String tm_name;
    private String category3_name;

    public PaymentWide(PaymentInfo paymentInfo, OrderWide orderWide) {
        mergeOrderWide(orderWide);
        mergePaymentInfo(paymentInfo);

    }

    public void mergePaymentInfo(PaymentInfo paymentInfo) {
        if (paymentInfo != null) {
            try {
                BeanUtils.copyProperties(this, paymentInfo);
                this.payment_create_time = paymentInfo.getCreate_time();
                this.payment_id = paymentInfo.getId();
            } catch (IllegalAccessException | InvocationTargetException e) {
                e.printStackTrace();
            }
        }
    }

    public void mergeOrderWide(OrderWide orderWide) {
        if (orderWide != null) {
            try {
                BeanUtils.copyProperties(this, orderWide);
                this.order_create_time = orderWide.getCreate_time();
            } catch (IllegalAccessException | InvocationTargetException e) {
                e.printStackTrace();
            }
        }
    }
}
