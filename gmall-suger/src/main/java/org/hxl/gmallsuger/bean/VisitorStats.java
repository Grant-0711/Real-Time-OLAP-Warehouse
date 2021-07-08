package org.hxl.gmallsuger.bean;

/**
 * @author Grant
 * @create 2021-07-08 9:51
 */
import lombok.Data;
import lombok.NoArgsConstructor;


@Data
@NoArgsConstructor
public class VisitorStats {
    private String hour;
    private String is_new;
    private Long pv;
    private Long uv;
    private Long uj;
}
