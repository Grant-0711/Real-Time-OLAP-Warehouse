package org.hxl.gmallsuger.controller;

/**
 * @author Grant
 * @create 2021-07-08 9:51
 */
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.hxl.gmallsuger.bean.*;
import org.hxl.gmallsuger.service.KeywordStatsService;
import org.hxl.gmallsuger.service.ProductStatsService;
import org.hxl.gmallsuger.service.ProvinceStatsService;
import org.hxl.gmallsuger.service.VisitorStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigInteger;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


@RestController
public class SugarController {

    public int defaultDate(int date) {
        if (date == 0) { // 表示请求这个地址的时候没有传递日期, 可以认为他正在请求当天的数据
            date = Integer.parseInt(new SimpleDateFormat("YYYYMMdd").format(new Date()));

        }
        return date;
    }

    @Autowired
    ProductStatsService product;

    @RequestMapping("/suger/gmv")
    public String gmv(@RequestParam(value = "date", defaultValue = "0") int date) {
        date = defaultDate(date);

        JSONObject result = new JSONObject();
        result.put("status", 0);
        result.put("msg", "");
        result.put("data", product.getGMV(date));
        return result.toJSONString();
    }

    @RequestMapping("/suger/tm_gmv")
    public String tmGmv(@RequestParam(value = "date", defaultValue = "0") int date,
                        @RequestParam(value = "limit", defaultValue = "5") int limit) {
        date = defaultDate(date);

        List<TmStats> list = product.getGMVByTM(date, limit);

        JSONObject result = new JSONObject();
        result.put("status", 0);
        result.put("msg", "");

        JSONObject data = new JSONObject();
        JSONArray categories = new JSONArray();
        for (TmStats tmStats : list) {
            categories.add(tmStats.getTm_name());
        }
        data.put("categories", categories);

        JSONArray series = new JSONArray();

        JSONObject obj = new JSONObject();
        obj.put("name", "商品品牌");
        JSONArray data1 = new JSONArray();
        for (TmStats tmStats : list) {
            data1.add(tmStats.getOrder_amount());
        }
        obj.put("data", data1);
        series.add(obj);
        data.put("series", series);

        result.put("data", data);

        return result.toJSONString();
    }

    @RequestMapping("/suger/c3_gmv")
    public String c3Gmv(@RequestParam(value = "date", defaultValue = "0") int date,
                        @RequestParam(value = "limit", defaultValue = "5") int limit) {
        date = defaultDate(date);

        List<C3Stats> list = product.getGMVByC3(date, limit);

        JSONObject result = new JSONObject();
        result.put("status", 0);
        result.put("msg", "");

        JSONArray data = new JSONArray();
        for (C3Stats c3Stats : list) {
            JSONObject obj = new JSONObject();
            obj.put("name", c3Stats.getCategory3_name());
            obj.put("value", c3Stats.getOrder_amount());

            data.add(obj);
        }

        result.put("data", data);

        return result.toJSONString();
    }

    @RequestMapping("/suger/spu_gmv")
    public String spuGmv(@RequestParam(value = "date", defaultValue = "0") int date,
                         @RequestParam(value = "limit", defaultValue = "100") int limit) {
        date = defaultDate(date);

        List<SpuStats> list = product.getGMVBySpu(date, limit);

        JSONObject result = new JSONObject();
        result.put("status", 0);
        result.put("msg", "");

        JSONObject data = new JSONObject();

        JSONArray columns = new JSONArray();
        data.put("columns", columns);
        JSONObject c1 = new JSONObject();
        c1.put("name", "spu名字");
        c1.put("id", "spu_name");
        columns.add(c1);
        JSONObject c2 = new JSONObject();
        c2.put("name", "销售额");
        c2.put("id", "order_amount");
        columns.add(c2);

        JSONArray rows = new JSONArray();
        for (SpuStats spuStats : list) {
            JSONObject row = new JSONObject();
            row.put("spu_name", spuStats.getSpu_name());
            row.put("order_amount", spuStats.getOrder_amount());
            rows.add(row);
        }

        data.put("rows", rows);

        result.put("data", data);

        return result.toJSONString();
    }

    @Autowired
    ProvinceStatsService province;

    @RequestMapping("/suger/province_map")
    public String provinceMap(@RequestParam(value = "date", defaultValue = "0") int date) {
        date = defaultDate(date);

        List<ProvinceStats> list = province.getProvinceStatsByProvinceName(date);

        JSONObject result = new JSONObject();
        result.put("status", 0);
        result.put("msg", "");

        JSONObject data = new JSONObject();

        JSONArray mapData = new JSONArray();
        for (ProvinceStats ps : list) {
            JSONObject obj = new JSONObject();
            obj.put("name", ps.getProvince_name());
            obj.put("value", ps.getOrder_amount());

            JSONArray tooltipValues = new JSONArray();
            tooltipValues.add(ps.getOrder_count());
            obj.put("tooltipValues", tooltipValues);

            mapData.add(obj);
        }
        data.put("mapData", mapData);

        data.put("valueName", "销售额");

        JSONArray tooltipNames = new JSONArray();
        tooltipNames.add("订单数");
        data.put("tooltipNames", tooltipNames);

        JSONArray tooltipUnits = new JSONArray();
        tooltipUnits.add("个");
        data.put("tooltipUnits", tooltipUnits);

        result.put("data", data);

        return result.toJSONString();
    }

    @Autowired
    VisitorStatsService visitor;

    @RequestMapping("/suger/visitor")
    public String visitor(@RequestParam(value = "date", defaultValue = "0") int date) {
        DecimalFormat df = new DecimalFormat("00");
        date = defaultDate(date);

        List<VisitorStats> list = visitor.statsByHour(date);
        // key:hour  value:VisitorStats
        HashMap<String, VisitorStats> map = new HashMap<>();
        for (VisitorStats vs : list) {
            map.put(vs.getHour(), vs);
        }

        JSONObject result = new JSONObject();
        result.put("status", 0);
        result.put("msg", "");

        JSONObject data = new JSONObject();
        JSONArray categories = new JSONArray();
        for (int i = 0; i < 24; i++) {
            //categories.add(i);  // 00 01.. 11 12 ...
            categories.add(df.format(i));
        }
        data.put("categories", categories);

        // 添加折线图
        JSONArray series = new JSONArray();

        JSONObject pv = new JSONObject();
        pv.put("name", "pv");
        JSONArray innerData = new JSONArray();
        for (int i = 0; i < 24; i++) {
            VisitorStats vs = map.get(i + "");
            if (vs == null) {
                innerData.add(0);
            } else {
                innerData.add(vs.getPv());
            }
        }
        pv.put("data", innerData);

        series.add(pv);

        JSONObject uv = new JSONObject();
        uv.put("name", "uv");
        JSONArray innerData2 = new JSONArray();
        for (int i = 0; i < 24; i++) {
            VisitorStats vs = map.get(i + "");
            if (vs == null) {
                innerData2.add(0);
            } else {
                innerData2.add(vs.getUv());
            }
        }
        uv.put("data", innerData2);

        series.add(uv);

        JSONObject uj = new JSONObject();
        uj.put("name", "uj");
        JSONArray innerData3 = new JSONArray();
        for (int i = 0; i < 24; i++) {
            VisitorStats vs = map.get(i + "");
            if (vs == null) {
                innerData3.add(0);
            } else {
                innerData3.add(vs.getUj());
            }
        }
        uj.put("data", innerData3);

        series.add(uj);

        data.put("series", series);
        result.put("data", data);

        return result.toJSONString();
    }

    @RequestMapping("/suger/visitor_is_new")
    public String visitor_is_new(@RequestParam(value = "date", defaultValue = "0") int date) {
        DecimalFormat df = new DecimalFormat("00");
        date = defaultDate(date);

        List<VisitorStats> list = visitor.statsByIsNew(date);

        JSONObject result = new JSONObject();
        result.put("status", 0);
        result.put("msg", "");

        JSONObject data = new JSONObject();
        data.put("total", list.size());

        JSONArray columns = new JSONArray();

        columns.add("{\"name\": \"新老用户\", \"id\": \"new_or_old\"}");
        columns.add("{\"name\": \"pv\", \"id\": \"pv\"}");
        columns.add("{\"name\": \"uv\", \"id\": \"uv\"}");
        columns.add("{\"name\": \"uj\", \"id\": \"uj\"}");

        data.put("columns", columns);

        JSONArray rows = new JSONArray();
        for (VisitorStats vs : list) {

            JSONObject row = new JSONObject();
            row.put("new_or_old", vs.getIs_new());
            row.put("pv", vs.getPv());
            row.put("uv", vs.getUv());
            row.put("uj", vs.getUj());

            rows.add(row);
        }

        data.put("rows", rows);

        result.put("data", data);

        return result.toJSONString().replaceAll("\"\\{", "{").replaceAll("}\"", "}").replaceAll("\\\\", "");
    }

    @Autowired
    KeywordStatsService kwService;

    @RequestMapping("/suger/kw")
    public String k(@RequestParam(value = "date", defaultValue = "0") int date,
                    @RequestParam(value = "limit", defaultValue = "10") int limit) {

        date = defaultDate(date);
        final Map<String, BigInteger> kwMap = kwService.statsKeyword(date, limit);

        JSONObject result = new JSONObject();
        result.put("status", 0);
        result.put("msg", "");

        JSONArray data = new JSONArray();

        kwMap.forEach((key, value) -> {
            JSONObject obj = new JSONObject();
            obj.put("name", key);
            obj.put("value", value);

            data.add(obj);
        });

        result.put("data", data);

        return result.toJSONString();
    }

}
