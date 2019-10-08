package com.henry.report.util;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import sun.security.krb5.Config;

import java.util.HashMap;
import java.util.Map;

/**
 * @Author: Henry
 * @Description: KafkaProducerConfig
 * @Date: Create in 2019/10/6 21:56
 **/

@Configuration  // 表示该类是一个配置类
public class KafkaProducerConfig {

    // 通过@value注解将配置文件中kafka.bootstrap_servers_config的值赋值给成员变量
    @Value("${kafka.bootstrap_servers_config}")
    private String bootstrap_servers_config;

    // 如果出现发送失败的情况，允许重试的次数
    @Value("${kafka.retries_config}")
    private String retries_config;

    //  每个批次发送多大的数据，单位：字节
    @Value("${kafka.batch_size}")
    private String batch_size;

    //      定时发送，达到 1ms 发送
    @Value("${kafka.linger_ms_config}")
    private String linger_ms_config;

    //     缓存的大小，单位：字节
    @Value("${kafka.buffer_memory_config}")
    private String buffer_memory_config;

    //     TOPOC 名字
    @Value("${kafka.topic}")
    private String topic;


    @Bean   // 表示该对象是受 Spring 管理的一个 Bean
    public KafkaTemplate kafkaTemplate(){

        // 构建工程需要的配置
        Map<String, Object> configs = new HashMap<>();

        // 将成员变量的值设置到Map中，在创建kafka_producer中用到
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap_servers_config);
        //创建生产者工程
        ProducerFactory<String, String> producerFactory = new DefaultKafkaProducerFactory(configs) ;

        // 表示需要返回一个 kafkaTemplate 对象
        return new KafkaTemplate(producerFactory);
    }
}

























