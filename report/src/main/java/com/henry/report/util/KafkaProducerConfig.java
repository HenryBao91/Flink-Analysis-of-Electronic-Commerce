package com.henry.report.util;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * @Author: Henry
 * @Description: KafkaProducerConfig
 * @Date: Create in 2019/10/6 21:56
 **/

@Configuration  // 1、表示该类是一个配置类，这样在下面才能创建 Bean
public class KafkaProducerConfig {

    // 通过@value注解将配置文件中kafka.bootstrap_servers_config的值赋值给成员变量
    @Value("${kafka.bootstrap_servers_config}")
    private String bootstrap_servers_config;
    // 如果出现发送失败的情况，允许重试的次数
    @Value("${kafka.retries_config}")
    private String retries_config;
    //  每个批次发送多大的数据，单位：字节
    @Value("${kafka.batch_size_config}")
    private String batch_size_config;
    //      定时发送，达到 1ms 发送
    @Value("${kafka.linger_ms_config}")
    private String linger_ms_config;
    //     缓存的大小，单位：字节
    @Value("${kafka.buffer_memory_config}")
    private String buffer_memory_config;
    //     TOPOC 名字
    @Value("${kafka.topic}")
    private String topic;


    @Bean   // 2、表示该对象是受 Spring 管理的一个 Bean
    public KafkaTemplate kafkaTemplate() {

        // 构建工程需要的配置
        Map<String, Object> configs = new HashMap<>();

        // 3、设置相应的配置
        // 将成员变量的值设置到Map中，在创建kafka_producer中用到
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap_servers_config);
        configs.put(ProducerConfig.RETRIES_CONFIG, retries_config);
        configs.put(ProducerConfig.BATCH_SIZE_CONFIG, batch_size_config);
        configs.put(ProducerConfig.LINGER_MS_CONFIG, linger_ms_config);
        configs.put(ProducerConfig.BUFFER_MEMORY_CONFIG, buffer_memory_config);

        // 设置 key、value 的序列化器
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG , StringSerializer.class);
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG , StringSerializer.class);

        // 指定自定义分区
        configs.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, RoundRobinPartitioner.class);


        // 4、创建生产者工厂
        ProducerFactory<String, String> producerFactory = new DefaultKafkaProducerFactory(configs);

        // 5、再把工厂传递给Template构造方法
        // 表示需要返回一个 kafkaTemplate 对象
        return new KafkaTemplate(producerFactory);
    }
}

























