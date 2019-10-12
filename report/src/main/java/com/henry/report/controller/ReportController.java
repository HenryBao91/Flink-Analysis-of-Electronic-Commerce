package com.henry.report.controller;

import com.alibaba.fastjson.JSON;
import com.henry.report.bean.Message;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;

/**
 * @Author: Henry
 * @Description:
 * @Date: Create in 2019/10/11 23:43
 **/

// 表示这是一个 Controller，并且其中所有的方法都是带有 @ResponseBody 的注解
@RestController
public class ReportController {

    @Autowired
    KafkaTemplate kafkaTemplate;

    @RequestMapping("/receive")
    public Map<String, String> receive(@RequestBody String json) {

        Map<String, String> map = new HashMap();   // 记录是否发送成功

        try {
            // 构建 Message
            Message msg = new Message();
            msg.setMessage(json);
            msg.setCount(1);
            msg.setTimestamp(System.currentTimeMillis());

            String msgJSON = JSON.toJSONString(msg);

            // 发送 Message 到 Kafka
            kafkaTemplate.send("pyg", msgJSON);
            map.put("success", "ture");

        }catch (Exception ex){
            ex.printStackTrace();
            map.put("success", "false");
        }

        return map;
    }

}
