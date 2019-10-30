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
  * @Description: 渠道 PV/UV
  *              1、字段转换；
  *              2、分组；
  *              3、时间窗口；
  *              4、聚合；
  *              5、落地HBase
  * @Date: Create in 2019/10/30 20:15 
  **/

case class ChannelPvUv(
                      val channelId: String,
                      val yearDayMonthHour: String,
                      val pv: Long,
                      val uv: Long
                      )

object ChannelPvUvTask {

  def process(clickLogWideDateStream : DataStream[ClickLogWide])= {

    // 1、转换
    val channelPvUvDS: DataStream[ChannelPvUv] = clickLogWideDateStream.map{
      clickLogWide => {
        ChannelPvUv(clickLogWide.channelID, clickLogWide.yearMonthDayHour,
          clickLogWide.count, clickLogWide.isHourNew)
      }
    }

    // 2、分组
    val keyedStream: KeyedStream[ChannelPvUv, String] = channelPvUvDS.keyBy{
      channelPvUv => channelPvUv.channelId + channelPvUv.yearDayMonthHour
    }

    // 3、窗口
    val windowedStream: WindowedStream[ChannelPvUv, String, TimeWindow] =
      keyedStream.timeWindow(Time.seconds(3))


    // 4、聚合
    val reduceDataStream: DataStream[ChannelPvUv] = windowedStream.reduce{
      (t1, t2) => ChannelPvUv(t1.channelId, t1.yearDayMonthHour, t1.pv + t2.pv, t1.uv + t2.uv)
    }

    // 5、HBase 落地
    reduceDataStream.addSink(new SinkFunction[ChannelPvUv] {

      override def invoke(value: ChannelPvUv): Unit = {

        // HBase 相关字段
        val tableName = "channel_pvuv"
        val clfName = "info"
        val channelIdColumn = "channelId"
        val yearMonthDayHourColumn = "yearMonthDayHour"
        val pvColumn = "pv"
        val uvColumn = "uv"

        val rowkey = value.channelId + ":" + value.yearDayMonthHour

        // 查询 HBase ，并且获取相关记录
        val pvInHBase: String = HBaseUtil.getData(tableName, rowkey, clfName, pvColumn)
        val uvInHBase: String = HBaseUtil.getData(tableName, rowkey, clfName, uvColumn)

        var totalPv = 0L
        var totalUv = 0L

        // 如果 HBase 中没有 PV 值，就把当前值保存；如果有值就进行累加
        if(StringUtils.isBlank(pvInHBase)){
          totalPv = value.pv
        }
        else {
          totalPv = pvInHBase.toLong + value.pv
        }

        // 如果 HBase 中没有 UV 值，就把当前值保存；如果有值就进行累加
        if(StringUtils.isBlank(uvInHBase)){
          totalUv = value.uv
        }
        else {
          totalUv = uvInHBase.toLong + value.uv
        }

        // 保存数据
        HBaseUtil.putMapData(tableName, rowkey, clfName, Map(

          channelIdColumn -> value.channelId ,
            yearMonthDayHourColumn -> value.yearDayMonthHour ,
            pvColumn -> value.pv.toString ,
            uvColumn -> value.uv.toString
        ))

      }
    })
  }
}
