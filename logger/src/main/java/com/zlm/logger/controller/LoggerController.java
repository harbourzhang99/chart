package com.zlm.logger.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.*;

/**
 * @author Harbour
 * @date 2021-03-24 21:00
 */

@RestController
@Slf4j
public class LoggerController {

    @Autowired
    KafkaTemplate<String, String> kafkaTemplate;

    @RequestMapping("/applog")
    public String applog(@RequestBody String mockLog) {
//        System.out.println(mockLog);
//        FileOutputStream stream = null;
//        FileWriter fw = null;
//        try {
//             fw = new FileWriter("app.log");
//             while (mockLog != null) {
//                 fw.append(mockLog);
//                 fw.append("\n");
//             }
//        } catch (Exception e) {
//            e.printStackTrace();
//        } finally {
//            try {
//                fw.close();
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//        }
        JSONObject jsonObject = JSON.parseObject(mockLog);
        if (jsonObject.getJSONObject("start") != null) {
            kafkaTemplate.send("mall-start", mockLog);
        } else {
            kafkaTemplate.send("mall-event", mockLog);
        }
        log.info(mockLog);
        return "success";
    }

}
