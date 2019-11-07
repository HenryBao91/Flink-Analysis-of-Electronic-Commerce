package com.henry.syncdb

import java.util.Properties

import com.henry.syncdb.bean.{Cannal, HBaseOperation}
import com.henry.syncdb.task.PreprocessTask
import com.henry.syncdb.util.{FlinkUtils, GlobalConfigutil, HBaseUtil}
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
//    kafkaDataStream.print()

    val cannalDs: DataStream[Cannal] = kafkaDataStream.map {
      json =>
        Cannal(json)
    }
//    cannalDs.print()


    val waterDS: DataStream[Cannal] = cannalDs.assignTimestampsAndWatermarks(
      new AssignerWithPeriodicWatermarks[Cannal] {

      //  当前的时间戳
      var currentTimestamp = 0L

      //  延迟的时间
      val delayTime = 2000L

      //  返回水印时间
      override def getCurrentWatermark: Watermark = {
        new Watermark(currentTimestamp - delayTime)
      }

      //  比较当前元素的时间和上一个元素的时间，取最大值，防止发生时光倒流
      override def extractTimestamp(element: Cannal, previousElementTimestamp: Long): Long = {
        currentTimestamp = Math.max(element.timestamp, previousElementTimestamp)
        currentTimestamp
      }
    })
//    waterDS.print()

    val hbaseDs: DataStream[HBaseOperation] = PreprocessTask.process(waterDS)
    hbaseDs.print()

    hbaseDs.addSink(new SinkFunction[HBaseOperation] {
      override def invoke(value: HBaseOperation): Unit = {
        value.opType match {
          case "DELETE" => HBaseUtil.deleteData(value.tableName,value.rowkey,value.cfName)
          case _ => HBaseUtil.putData(value.tableName,value.rowkey,value.cfName,value.colName,value.colValue)
        }
      }
    })



    // 执行任务
    env.execute("sync-db")

  }
}
