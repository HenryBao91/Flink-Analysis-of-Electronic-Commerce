package com.henry.realprocess.task
import com.henry.realprocess.bean.ClickLogWide
import com.henry.realprocess.util.HBaseUtil
import org.apache.commons.lang.StringUtils
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, WindowedStream}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.windowing.time.Time


/**
  * @Author: Henry
  * @Description:
  * @Date: Create in 2019/11/3 15:52 
  **/

// 2. 添加一个`ChannelNetwork`样例类，它封装要统计的四个业务字段：频道ID（channelID）、运营商
// （network）、日期（date）pv、uv、新用户（newCount）、老用户（oldCount）
case class ChannelNetWork(
                        var channelId: String,
                        var network: String,
                        var date: String,
                        var pv: Long,
                        var uv: Long,
                        var newCount: Long,
                        var oldCount: Long
                         )


object ChannelNetworkTask extends BaseTask[ChannelNetWork]{

  override def map(clickLogWideDataStream: DataStream[ClickLogWide]): DataStream[ChannelNetWork] = {

    val isOld = (isNew: Int, isDateNew: Int) => if (isNew == 0 && isDateNew == 1) 1 else 0

    clickLogWideDataStream.flatMap{
      clickLogWide => {
        List(
          ChannelNetWork(      //  月维度
            clickLogWide.channelID,
            clickLogWide.network,
            clickLogWide.yearMonth,
            clickLogWide.count,
            clickLogWide.isMonthNew,
            clickLogWide.isNew,
            isOld(clickLogWide.isNew, clickLogWide.isMonthNew)
          ),
          ChannelNetWork(      //  天维度
            clickLogWide.channelID,
            clickLogWide.network,
            clickLogWide.yearMonthDay,
            clickLogWide.count,
            clickLogWide.isDayNew,
            clickLogWide.isNew,
            isOld(clickLogWide.isNew, clickLogWide.isDayNew)
          ),
          ChannelNetWork(      //  小时维度
            clickLogWide.channelID,
            clickLogWide.network,
            clickLogWide.yearMonthDayHour,
            clickLogWide.count,
            clickLogWide.isHourNew,
            clickLogWide.isNew,
            isOld(clickLogWide.isNew, clickLogWide.isHourNew)
          )
        )
      }
    }
  }

  override def keyBy(mapDataStream: DataStream[ChannelNetWork]): KeyedStream[ChannelNetWork, String] = {

      mapDataStream.keyBy {
        network =>
          network.channelId +" : "+ network.network +" : "+ network.date
      }
  }

  override def reduce(windowedStream: WindowedStream[ChannelNetWork, String, TimeWindow]): DataStream[ChannelNetWork] = {
    windowedStream.reduce {
      (t1, t2) => {
        ChannelNetWork(
          t1.channelId,
          t1.network,
          t1.date,
          t1.pv + t2.pv,
          t1.uv + t2.uv,
          t1.newCount + t2.newCount,
          t1.oldCount + t2.oldCount
        )
      }
    }
  }


  override def sink2HBase(reduceDataStream: DataStream[ChannelNetWork]): Unit = {

    reduceDataStream.addSink(
      network => {
        // 创建 HBase 相关列 - 准备hbase的表名、列族名、rowkey名、列名
        val tableName = "channel_network"
        val clfName = "info"
        // 频道ID（channelID）、运营商（network）、日期（date）pv、uv、新用户（newCount）、老用户(oldCount）
        val rowkey = s"${network.channelId} : ${network.date} : ${network.network}"   // 引用变量的方式
        val channelIdColName = "channelID"
        val networkColName = "network"
        val dateColName = "date"
        val pvColName = "pv"
        val uvColName = "uv"
        val newCountColName = "newCount"
        val oldCountColName = "oldCount"

        // 查询 HBase
        // - 判断hbase中是否已经存在结果记录
        val resultMap: Map[String, String] = HBaseUtil.getMapData(tableName, rowkey, clfName,
          List( pvColName, uvColName, newCountColName, oldCountColName )
        )

        // 数据累加
        var totalPv = 0L
        var totalUv = 0L
        var totalNewCount = 0L
        var totalOldCount = 0L

        // totalPv
        if (resultMap != null && resultMap.size > 0 && StringUtils.isNotBlank(resultMap.getOrElse(pvColName,""))) {
          totalPv = resultMap(pvColName).toLong + network.pv
        }
        else {
          totalPv = network.pv
        }

        // totalUv
        if (resultMap != null && resultMap.size > 0 && StringUtils.isNotBlank(resultMap.getOrElse(uvColName,""))) {
          totalUv = resultMap(uvColName).toLong + network.uv
        }
        else {
          totalUv = network.uv
        }

        // totalNewCount
        if (resultMap != null && resultMap.size > 0 && StringUtils.isNotBlank(resultMap.getOrElse(newCountColName,""))) {
          totalNewCount = resultMap(newCountColName).toLong + network.newCount
        }
        else {
          totalNewCount = network.newCount
        }

        // totalOldCount
        if (resultMap != null && resultMap.size > 0 && StringUtils.isNotBlank(resultMap.getOrElse(oldCountColName,""))) {
          totalOldCount = resultMap(oldCountColName).toLong + network.oldCount
        }
        else {
          totalOldCount = network.oldCount
        }

        // 保存数据
        HBaseUtil.putMapData(
            tableName, rowkey, clfName, Map(
            channelIdColName -> network.channelId,
            networkColName -> network.network,
            dateColName -> network.date,
            pvColName -> totalPv,
            uvColName -> totalUv,
            newCountColName -> totalNewCount,
            oldCountColName -> totalOldCount
          )
        )
      }
    )
  }
}
