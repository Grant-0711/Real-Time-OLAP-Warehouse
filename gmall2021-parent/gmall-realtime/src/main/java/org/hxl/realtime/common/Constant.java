package org.hxl.realtime.common;

/**
 * @author Grant
 * @create 2021-06-25 17:47
 */
public class Constant {
    public static final String PHOENIX_DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver";
    public static final String PHOENIX_URL = "jdbc:phoenix:hadoop107,hadoop108,hadoop109:2181";
    public static final String MYSQL_DRIVER = "com.mysql.jdbc.Driver";
    public static final String MYSQL_URL = "jdbc:mysql://hadoop107:3306/gmall2021?user=root&password=123456";


    public static final String TOPIC_ODS_LOG = "ods_log";
    public static final String TOPIC_DWD_START_LOG = "dwd_start_log";
    public static final String TOPIC_DWD_PAGE_LOG = "dwd_page_log";
    public static final String TOPIC_DWD_DISPLAY_LOG = "dwd_display_log";
    public static final String TOPIC_ODS_DB = "ods_db";
    public static final String TOPIC_DWM_UV = "dwm_uv";
    public static final String TOPIC_DWM_USER_JUMP_DETAIL = "dwm_user_jump_detail";
    public static final String TOPIC_DWD_ORDER_INFO = "dwd_order_info";
    public static final String TOPIC_DWD_ORDER_DETAIL = "dwd_order_detail";

    // phoenix中的维度表
    public static final String DIM_USER_INFO = "DIM_USER_INFO";
    public static final String DIM_BASE_PROVINCE = "DIM_BASE_PROVINCE";
    public static final String DIM_SKU_INFO = "DIM_SKU_INFO";
    public static final String DIM_SPU_INFO = "DIM_SPU_INFO";
    public static final String DIM_BASE_TRADEMARK = "DIM_BASE_TRADEMARK";
    public static final String DIM_BASE_CATEGORY3 = "DIM_BASE_CATEGORY3";

    public static final int DIM_EXPIRE_SECOND = 24 * 60 * 60;  // 维度数据在redis的过期时间默认是24小时


}
