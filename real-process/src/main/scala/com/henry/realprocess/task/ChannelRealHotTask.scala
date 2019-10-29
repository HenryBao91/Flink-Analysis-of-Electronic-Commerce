package com.henry.realprocess.task

import com.henry.realprocess.bean.ClickLogWide
import com.henry.realprocess.util.HBaseUtil
import org.apache.flink.streaming.api.scala.{DataStream, WindowedStream}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala.KeyedStream
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.commons.lang.StringUtils


/**
  * @Author: Henry
  * @Description: 频道热点分析业务开发
  *              1、字段转换；
  *              2、分组；
  *              3、时间窗口；
  *              4、聚合；
  *              5、落地HBase
  * @Date: Create in 2019/10/29 20:22 
  **/

case class ChannelRealHot(var channelid:String, var visited:Long)


object ChannelRealHotTask {

  def process(clickLogWideDateStream : DataStream[ClickLogWide])= {

    //  1、字段转换 channelid、visited
    val realHotDataStream: DataStream[ChannelRealHot] = clickLogWideDateStream.map{
      clickLogWide: ClickLogWide =>
        ChannelRealHot(clickLogWide.channelID, clickLogWide.count)
    }

    // 2、分组
    val keyedStream: KeyedStream[ChannelRealHot, String] = realHotDataStream.keyBy(_.channelid)


    // 3、时间窗口
    val windowedStream: WindowedStream[ChannelRealHot, String, TimeWindow] = keyedStream.timeWindow(
      Time.seconds(3))

    // 4、聚合
    val reduceDataStream: DataStream[ChannelRealHot] = windowedStream.reduce{
      (t1: ChannelRealHot, t2: ChannelRealHot) =>
        ChannelRealHot(t1.channelid, t1.visited + t2.visited)
    }
    // 输出测试
    reduceDataStream

    // 5、落地 HBase
    reduceDataStream.addSink(new SinkFunction[ChannelRealHot] {

      override def invoke(value: ChannelRealHot): Unit = {

        // HBase 相关字段
        val tableName = "channel"
        val clfName = "info"
        val channelIdColumn = "channelId"
        val visitedColumn = "visited"
        val rowkey = value.channelid


        // 查询 HBase ，并且获取相关记录
        val visitedValue: String = HBaseUtil.getData(tableName, rowkey, clfName, visitedColumn)
        // 创建总数的临时变量
        var totalCount: Long = 0

        if(StringUtils.isBlank(visitedValue)){
          totalCount = value.visited
        }
        else {
          totalCount = visitedValue.toLong + value.visited
        }

        // 保存数据
        HBaseUtil.putMapData(tableName, rowkey, clfName, Map(
          channelIdColumn -> value.channelid ,
          visitedColumn -> totalCount.toString
        ))
      }
    })
  }

}
