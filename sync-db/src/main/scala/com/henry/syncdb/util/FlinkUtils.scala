package com.henry.syncdb.util

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010

/**
  * @Author: Henry
  * @Description:
  * @Date: Create in 2019/11/6 21:58 
  **/
object FlinkUtils {

  //  初始化Flink流式环境
  def initFlinkEnv()={
    // Flink 流式环境的创建
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 设置env的处理时间为EventTime
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //  设置并行度
    env.setParallelism(1)

    //  设置checkpoint
    //  开启checkpoint，间隔时间为 5s
    env.enableCheckpointing(5000)
    //  设置处理模式，这句话可以省略，因为在上一个方法enableCheckpointing中默认设置的是EXACTLY_ONCE
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    //  设置两次checkpoint的间隔
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(1000)
    //  设置超时时长
    env.getCheckpointConfig.setCheckpointTimeout(60000)
    //  设置并行度
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)

    //  当程序关闭的时候，触发额外的 checkpoint
    env.getCheckpointConfig.enableExternalizedCheckpoints(
      CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

    //  设置检查点在HDFS中的存储位置
    env.setStateBackend(new FsStateBackend("hdfs://master:9000/flink-checkpoint"))

    env
  }


  // 整合 kafka
  def initKafkaFlink()={

    val props:Properties = new Properties()

    props.setProperty("bootstrap.servers", GlobalConfigutil.bootstrapServers)
    props.setProperty("group.id", GlobalConfigutil.gruopId)
    props.setProperty("enable.auto.commit", GlobalConfigutil.enableAutoCommit)
    props.setProperty("auto.commit.interval.ms", GlobalConfigutil.autoCommitIntervalMs)
    props.setProperty("auto.offset.reset", GlobalConfigutil.autoOffsetReset)

    // topic: String, valueDeserializer: DeserializationSchema[T], props: Properties
    val consumer: FlinkKafkaConsumer010[String] = new FlinkKafkaConsumer010[String](
      GlobalConfigutil.inputTopic,
      new SimpleStringSchema(),
      props
    )
    consumer
  }




}
