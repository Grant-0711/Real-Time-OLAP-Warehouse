package org.hxl.realtime.bean;

/**
 * @author Grant
 * @create 2021-07-05 4:37
 */
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ProvinceStats {

    private String stt;//窗口起始时间
    private String edt;  //窗口结束时间
    private Long province_id;
    private String province_name;
    private String area_code;
    private String iso_code;
    private String iso_3166_2;
    private BigDecimal order_amount;
    private Long order_count;
    private Long ts; //统计时间戳

}
