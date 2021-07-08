package org.hxl.gmallsuger.bean;

/**
 * @author Grant
 * @create 2021-07-08 9:50
 */
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Data
@NoArgsConstructor
public class ProvinceStats {
    private String province_name;
    private BigDecimal order_amount;
    private Long order_count;
}
