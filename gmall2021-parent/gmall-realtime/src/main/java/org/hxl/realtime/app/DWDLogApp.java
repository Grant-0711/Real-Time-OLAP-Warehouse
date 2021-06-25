package org.hxl.realtime.app;

/**
 * @author Grant
 * @create 2021-06-21 19:22
 */

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import org.hxl.realtime.app.BaseAppV1;
import org.hxl.realtime.common.Constant;

import org.hxl.realtime.util.FlinkSourceUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;




public class DWDLogApp  extends BaseAppV1{
    public static void main(String[] args) {
        new DWDLogApp().init(2001,
                2,
                "DwdLogApp",
                "DwdLogApp",
                Constant.TOPIC_ODS_LOG);

    }

    @Override
    public void run(StreamExecutionEnvironment env, DataStreamSource<String> sourceStream) {



        sourceStream.print();
    }
}