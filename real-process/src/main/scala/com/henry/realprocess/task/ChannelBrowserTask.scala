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

// 2. 添加一个`ChannelBrowser`样例类，它封装要统计的四个业务字段：频道ID（channelID）、运营商
// （browser）、日期（date）pv、uv、新用户（newCount）、老用户（oldCount）
case class ChannelBrowser(
                        var channelId: String,
                        var browser: String,
                        var date: String,
                        var pv: Long,
                        var uv: Long,
                        var newCount: Long,
                        var oldCount: Long
                         )


object ChannelBrowserTask extends BaseTask[ChannelBrowser]{

  override def map(clickLogWideDataStream: DataStream[ClickLogWide]): DataStream[ChannelBrowser] = {

    clickLogWideDataStream.flatMap{
      clickLogWide => {
        List(
          ChannelBrowser(      //  月维度
            clickLogWide.channelID,
            clickLogWide.browserType,
            clickLogWide.yearMonth,
            clickLogWide.count,
            clickLogWide.isMonthNew,
            clickLogWide.isNew,
            isOld(clickLogWide.isNew, clickLogWide.isMonthNew)
          ),
          ChannelBrowser(      //  天维度
            clickLogWide.channelID,
            clickLogWide.browserType,
            clickLogWide.yearMonthDay,
            clickLogWide.count,
            clickLogWide.isDayNew,
            clickLogWide.isNew,
            isOld(clickLogWide.isNew, clickLogWide.isDayNew)
          ),
          ChannelBrowser(      //  小时维度
            clickLogWide.channelID,
            clickLogWide.browserType,
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

  override def keyBy(mapDataStream: DataStream[ChannelBrowser]): KeyedStream[ChannelBrowser, String] = {

      mapDataStream.keyBy {
        browser =>
          browser.channelId +" : "+ browser.browser +" : "+ browser.date
      }
  }

  override def reduce(windowedStream: WindowedStream[ChannelBrowser, String, TimeWindow]): DataStream[ChannelBrowser] = {
    windowedStream.reduce {
      (t1, t2) => {
        ChannelBrowser(
          t1.channelId,
          t1.browser,
          t1.date,
          t1.pv + t2.pv,
          t1.uv + t2.uv,
          t1.newCount + t2.newCount,
          t1.oldCount + t2.oldCount
        )
      }
    }
  }


  override def sink2HBase(reduceDataStream: DataStream[ChannelBrowser]): Unit = {

    reduceDataStream.addSink(
      browser => {

        // 创建 HBase 相关列 - 准备hbase的表名、列族名、rowkey名、列名
        // 不需要加 val 或者 var ，因为引用的是父类的变量
        tableName = "channel_browser"
        rowkey = s"${browser.channelId} : ${browser.date} : ${browser.browser}"   // 引用变量的方式
        browserColName = "browser"


        // 查询 HBase
        // - 判断hbase中是否已经存在结果记录
        val resultMap: Map[String, String] = HBaseUtil.getMapData(tableName, rowkey, clfName,
          List( pvColName, uvColName, newCountColName, oldCountColName )
        )

        // 数据累加
        // 保存数据
        HBaseUtil.putMapData(
            tableName, rowkey, clfName, Map(
            channelIdColName -> browser.channelId,
            browserColName -> browser.browser,
            dateColName -> browser.date,
            pvColName -> getTotal(resultMap, pvColName , browser.pv),
            uvColName -> getTotal(resultMap, uvColName , browser.uv),
            newCountColName -> getTotal(resultMap, newCountColName , browser.newCount),
            oldCountColName -> getTotal(resultMap, oldCountColName , browser.newCount)
          )
        )
      }
    )
  }
}
