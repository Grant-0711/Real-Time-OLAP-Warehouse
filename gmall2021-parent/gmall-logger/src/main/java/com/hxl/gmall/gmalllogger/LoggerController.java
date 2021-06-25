package com.hxl.gmall.gmalllogger;

/**
 * @author Grant
 * @create 2021-06-16 9:57
 */
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;


import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
@RestController
@Slf4j
public class LoggerController {

    @Autowired
    KafkaTemplate kafka;

    @GetMapping("/applog")  // 提交路径
    public String doLog(@RequestParam("param") String logJson) {
        log.info(logJson);
        kafka.send("ods_log", logJson);
        return "ok";
    }


    private void sendToKafka(String logString) {
        kafka.send("ods_log", logString);
    }

    private void saveToDisk(String logString) {
        log.info(logString);
    }
}