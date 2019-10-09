package com.henry.report.util;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @Author: Henry
 * @Description: 自定义分区
 * @Date: Create in 2019/10/9 23:00
 **/

public class RoundRobinPartitioner implements Partitioner {

    // AtomicInteger 并发包下的多线程安全的整型类
    AtomicInteger counter = new AtomicInteger(0) ;


    // 返回值为分区号： 0、1、2
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {

        // 获取分区的数量
        Integer partitions =  cluster.partitionCountForTopic(topic) ;

        int curpartition = counter.incrementAndGet() % partitions ;  // 当前轮询的 partition 号

        if(counter.get() > 65535){
            counter.set(0);
        }

        return curpartition;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
