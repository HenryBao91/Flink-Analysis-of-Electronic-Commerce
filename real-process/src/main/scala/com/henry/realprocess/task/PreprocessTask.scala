package com.henry.realprocess.task

import com.henry.realprocess.bean.{ClickLogWide, Message}
import org.apache.commons.lang.time.FastDateFormat
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.api.scala._

/**
  * @Author: Henry
  * @Description: 预处理任务
  * @Date: Create in 2019/10/27 14:31 
  **/
object PreprocessTask {


  def process(watermarkDataStream:DataStream[Message])= {

    watermarkDataStream.map {

      msg =>
        // 转换时间
        val yearMonth: String = FastDateFormat.getInstance("yyyyMM").format(msg.timeStamp)
        val yearMonthDay: String = FastDateFormat.getInstance("yyyyMMdd").format(msg.timeStamp)
        val yearMonthDayHour: String = FastDateFormat.getInstance("yyyyMMddHH").format(msg.timeStamp)

        // 转换地区
        val address = msg.clickLog.country + msg.clickLog.province + msg.clickLog.city

        ClickLogWide(
            msg.clickLog.channelID,
            msg.clickLog.categoryID,
            msg.clickLog.produceID,
            msg.clickLog.country,
            msg.clickLog.province,
            msg.clickLog.city,
            msg.clickLog.network,
            msg.clickLog.source,
            msg.clickLog.browserType,
            msg.clickLog.entryTime,
            msg.clickLog.leaveTime,
            msg.clickLog.userID,

            msg.count,
            msg.timeStamp,
            address,
            yearMonth,
            yearMonthDay,
            yearMonthDayHour,
            0,
            0,
            0,
            0
        )
    }

  }
}
