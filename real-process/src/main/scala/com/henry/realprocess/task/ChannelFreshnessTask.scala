package com.henry.realprocess.task

import com.henry.realprocess.bean.ClickLogWide
import com.henry.realprocess.util.HBaseUtil
import org.apache.commons.lang.StringUtils
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, WindowedStream}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

/**
  * @Author: Henry
  * @Description:
  * @Date: Create in 2019/10/31 21:38
  **/

case class ChannelFreshness(
                               var channelId : String ,
                               var date : String ,
                               var newCount: Long ,
                               val oldCount: Long

                               )

/**
  * 1、  转换
  * 2、  分组
  * 3、  时间窗口
  * 4、  聚合
  * 5、  落地 HBase
  */
object ChannelFreshnessTask {

  def process(clickLogWideDataStream: DataStream[ClickLogWide])= {

    // 1、 转换
    val mapDataStream: DataStream[ChannelFreshness] = clickLogWideDataStream.flatMap {
      clickLog =>

        // 如果是老用户，只有在第一次来的时候，计数为 1
        val isOld = (isNew: Int, isDateNew: Int) => if (isNew == 0 && isDateNew == 1) 1 else 0
        //  统计新用户、老用户数量
        List(
          ChannelFreshness(clickLog.channelID, clickLog.yearMonthDayHour, clickLog.isNew, isOld(clickLog.isNew, clickLog.isHourNew)),
          ChannelFreshness(clickLog.channelID, clickLog.yearMonthDay, clickLog.isNew, isOld(clickLog.isNew, clickLog.isDayNew)),
          ChannelFreshness(clickLog.channelID, clickLog.yearMonth, clickLog.isNew, isOld(clickLog.isNew, clickLog.isMonthNew))
        )
    }

//     2、 分组
    val keyedStream: KeyedStream[ChannelFreshness, String] = mapDataStream.keyBy {
      freshness => (freshness.channelId + freshness.date)
    }


    // 3、 时间窗口
    val windowedStream: WindowedStream[ChannelFreshness, String, TimeWindow] = keyedStream.timeWindow(Time.seconds(3))


    // 4、 聚合
    val reduceDataStream: DataStream[ChannelFreshness] = windowedStream.reduce {
      (t1, t2) =>
        ChannelFreshness(t1.channelId, t1.date, t1.newCount + t2.newCount, t1.oldCount + t2.oldCount)
    }

    // 5、 落地 HBase
    reduceDataStream.addSink(new SinkFunction[ChannelFreshness] {
      override def invoke(value: ChannelFreshness): Unit = {
        // 创建 HBase 相关变量
        val tableName = "channel_freshness"
        val clfName = "info"
        val channelIdColumn = "channelId"
        val dateColumn = "date"
        val newCountColumn = "newCount"
        val oldCountColumn = "oldCount"

        val rowkey = value.channelId + ":" + value.date

        // 查询历史数据
        val resultMap: Map[String, String] = HBaseUtil.getMapData(tableName, rowkey, clfName, List(newCountColumn, oldCountColumn))

        // 累加
        var totalNewCount = 0L
        var totalOldCount = 0L

        if(resultMap != null && StringUtils.isNotBlank(resultMap.getOrElse(newCountColumn,""))){
          resultMap(newCountColumn).toLong + value.newCount
        }
        else {
          totalNewCount = value.newCount
        }

        if(resultMap != null && StringUtils.isNotBlank(resultMap.getOrElse(oldCountColumn,""))){
          resultMap(oldCountColumn).toLong + value.oldCount
        }
        else {
          totalOldCount = value.oldCount
        }


        // 保存数据
        HBaseUtil.putMapData(tableName, rowkey, clfName, Map(
          // 向如下列插入数据
          channelIdColumn -> value.channelId ,
          dateColumn -> value.date ,
          newCountColumn -> totalNewCount ,
          oldCountColumn -> totalOldCount
        ))

      }
    })
  }

}
