package com.henry.realprocess


import java.util.Properties

import com.alibaba.fastjson.JSON
import com.henry.realprocess.bean.{ClickLog, ClickLogWide, Message}
import com.henry.realprocess.task.PreprocessTask
import com.henry.realprocess.util.GlobalConfigutil
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010


/**
  * @Author: Henry
  * @Description: 入口类
  * @Date: Create in 2019/10/16 22:42 
  **/
object App {

  def main(args: Array[String]): Unit = {

    //------------ 初始化Flink流式环境,ctrl+alt+v --------------------
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // 设置处理时间为EventTime
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 设置并行度
    env.setParallelism(1)

    // 本地测试 加载本地集合 成为一个 Datastream 打印输出
    //    val localDataStream:DataStream[String] = env.fromCollection(
    //      List("hadoop", "hive", "hbase", "flink")
    //    )
    //    localDataStream.print()


    //------------ 添加 checkpoint 的支持 -------------------------------
    env.enableCheckpointing(5000) // 5秒启动一次checkpoint

    // 设置 checkpoint 只检查 1次，即 仅一次
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    // 设置两次 checkpoint 的最小时间间隔 1s
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(1000)
    // 设置checkpoint的超时时长, 60s
    env.getCheckpointConfig.setCheckpointTimeout(60000)
    // 允许的最大并行度
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    // 当程序关闭时，触发额外的checkpoint
    env.getCheckpointConfig.enableExternalizedCheckpoints(
      CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)


    // 设置checkpoint的地址
    env.setStateBackend(new FsStateBackend("hdfs://master:9000/flink-checkpoint/"))


    //--------------- 整合kafka --------------------------
    val properties = new Properties()
    //    kafka 集群地址
    properties.setProperty("bootstrap.servers", GlobalConfigutil.bootstrapServers)
    //     zookeeper 集群地址
    properties.setProperty("zookeeper.connect", GlobalConfigutil.zookeeperConnect)
    //     kafka topic
    properties.setProperty("input.topic", GlobalConfigutil.inputTopic)
    //     消费者组 ID
    properties.setProperty("gruop.id", GlobalConfigutil.gruopId)
    //     自动提交拉取到的消费端的消息offset到kafka
    properties.setProperty("enable.auto.commit", GlobalConfigutil.enableAutoCommit)
    //     自动提交offset到zookeeper的时间间隔单位（毫秒）
    properties.setProperty("auto.commit.interval.ms", GlobalConfigutil.autoCommitIntervalMs)
    //     每次消费最新的数据
    properties.setProperty("auto.offset.reset", GlobalConfigutil.autoOffsetReset)


    // topic 、反序列化器、 属性集合
    val consumer = new FlinkKafkaConsumer010[String](
      GlobalConfigutil.inputTopic,
      new SimpleStringSchema(),
      properties)

    val kafkaDataStream: DataStream[String] = env.addSource(consumer)

    //    kafkaDataStream.print()

    // JSON -> 元组
    val tupleDataStream = kafkaDataStream.map {
      msgJson =>
        val jsonObject = JSON.parseObject(msgJson)

        val message = jsonObject.getString("message")
        val count = jsonObject.getLong("count")
        val timeStamp = jsonObject.getLong("timestamp")

//        (message, count, timeStamp)
        // 改造成样例类
//        (ClickLog(message), count, timeStamp)
        Message(ClickLog(message), count, timeStamp)

    }

    tupleDataStream.print()

    //-----------------  添加水印支持  -----------------------

    var watermarkDataStream = tupleDataStream.assignTimestampsAndWatermarks(
      new AssignerWithPeriodicWatermarks[Message] {

        var currentTimestamp = 0L

        // 延迟时间
        var maxDelayTime = 2000L

        // 获取当前时间戳
        override def getCurrentWatermark: Watermark = {
          // 设置水印时间比事件时间小 2s
          new Watermark(currentTimestamp - maxDelayTime)
        }

        // 获取当前事件时间
        override def extractTimestamp(
                     element: Message,
                     previousElementTimestamp: Long): Long = {
          currentTimestamp = Math.max(element.timeStamp, previousElementTimestamp)
          currentTimestamp
        }
    }).print()

    //  数据的预处理
    val clickLogWideDateStream : DataStream[ClickLogWide] = PreprocessTask.process(watermarkDataStream)

    // 执行任务
    env.execute("real-process")
  }
}
