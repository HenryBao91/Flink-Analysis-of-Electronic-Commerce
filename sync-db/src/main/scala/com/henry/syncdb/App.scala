package com.henry.syncdb

import java.util.Properties

import com.henry.syncdb.util.{FlinkUtils, GlobalConfigutil}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010

/**
  * @Author: Henry
  * @Description:
  * @Date: Create in 2019/11/6 21:17 
  **/
object App {

  def main(args: Array[String]): Unit = {


    val env = FlinkUtils.initFlinkEnv()

    //    //  1、输出测试
    //    val testDs: DataStream[String] = env.fromCollection(List(
    //      "1", "2", "3"
    //    ))
    //    testDs.print()

    val consumer = FlinkUtils.initKafkaFlink()

    // 测试打印
    val kafkaDataStream: DataStream[String] = env.addSource(consumer)
    kafkaDataStream.print()



    // 执行任务
    env.execute("sync-db")

  }
}
