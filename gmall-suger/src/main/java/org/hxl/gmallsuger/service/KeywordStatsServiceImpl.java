package org.hxl.gmallsuger.service;

/**
 * @author Grant
 * @create 2021-07-08 9:55
 */
import org.hxl.gmallsuger.mapper.KeywordStatsMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class KeywordStatsServiceImpl implements KeywordStatsService {
    @Autowired
    KeywordStatsMapper mapper;

    @Override
    public Map<String, BigInteger> statsKeyword(int date, int limit) {
        List<Map<String, Object>> list = mapper.statsKeyword(date, limit);
        HashMap<String, BigInteger> result = new HashMap<>();
        for (Map<String, Object> map : list) {
            String key = map.get("keyword").toString();
            BigInteger value = (BigInteger) map.get("score");
            result.put(key, value);
        }

        return result;
    }
}
