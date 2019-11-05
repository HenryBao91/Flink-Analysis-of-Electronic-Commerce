package com.henry.canal_kafka.util;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.serializer.StringEncoder;

import java.util.Properties;

/**
 * Kafka生产消息工具类
 */
public class KafkaSender {
    private String topic;

    public KafkaSender(String topic){
        super();
        this.topic = topic;
    }

    /**
     * 发送消息到Kafka指定topic
     *
     * @param topic topic名字
     * @param key 键值
     * @param data 数据
     */
    public static void sendMessage(String topic , String key , String data){
        Producer<String, String> producer = createProducer();
        producer.send(new KeyedMessage<String , String>(topic , key , data));
    }

    private static Producer<String , String> createProducer(){
        Properties properties = new Properties();

        properties.put("metadata.broker.list" , GlobalConfigUtil.kafkaBootstrapServers);
        properties.put("zookeeper.connect" , GlobalConfigUtil.kafkaZookeeperConnect);
        properties.put("serializer.class" , StringEncoder.class.getName());

        return new Producer<String, String>(new ProducerConfig(properties));
    }
}