package org.hxl.gmallsuger.bean;

/**
 * @author Grant
 * @create 2021-07-08 9:49
 */

import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Data
@NoArgsConstructor
public class C3Stats {
    private String category3_name;
    private BigDecimal order_amount;
}
