package org.hxl.realtime.bean;

/**
 * @author Grant
 * @create 2021-07-05 18:32
 */
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class KeywordStats {

    private String stt;//窗口起始时间
    private String edt;  //窗口结束时间
    private String keyword;
    private String source;
    private Long ct;
    private Long ts; //统计时间戳

}
/*
 " date_format(tumble_start(et, interval '10' second), 'yyyy-MM-dd HH:mm:ss') stt, " +
                          " date_format(tumble_end(et, interval '10' second), 'yyyy-MM-dd HH:mm:ss') edt, " +
                          " word keyword, " +
                          " 'search' source, " +
                          " count(*) ct, " +
                          " unix_timestamp() * 1000 ts " +
 */
