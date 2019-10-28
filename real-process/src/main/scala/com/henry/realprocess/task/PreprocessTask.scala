package com.henry.realprocess.task

import com.henry.realprocess.bean.{ClickLogWide, Message}
import com.henry.realprocess.util.HBaseUtil
import org.apache.commons.lang.StringUtils
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

    /**
      *  大括号{}用于代码块，计算结果是代码最后一行；
      *  大括号{}用于拥有代码块的函数；
      *  大括号{}在只有一行代码时可以省略，除了case语句（Scala适用）；
      *  小括号()在函数只有一个参数时可以省略（Scala适用）；
      *  几乎没有二者都省略的情况。
      */
    watermarkDataStream.map {

      msg =>
        // 转换时间
        val yearMonth: String = FastDateFormat.getInstance("yyyyMM").format(msg.timeStamp)
        val yearMonthDay: String = FastDateFormat.getInstance("yyyyMMdd").format(msg.timeStamp)
        val yearMonthDayHour: String = FastDateFormat.getInstance("yyyyMMddHH").format(msg.timeStamp)

        // 转换地区
        val address = msg.clickLog.country + msg.clickLog.province + msg.clickLog.city

        val isNewtuple = isNewProcess(msg)

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
          isNewtuple._1,
          isNewtuple._2,
          isNewtuple._3,
          isNewtuple._4
        )
    }

  }

  /**
    * 判断用户是否为新用户
    * @param msg
    */
  private def isNewProcess(msg:Message)={

    // 1、定义4个变量，初始化为0
    var isNew = 0
    var isHourNew = 0
    var isDayNew = 0
    var isMonthNew = 0


    // 2、从HBase中查询用户记录，如果有记录，再去判断其他时间；如果没有记录，则证明是新用户
    val tableName = "user_history"
    var clfName = "info"
    var rowkey = msg.clickLog.userID + ":" + msg.clickLog.channelID

    //    - 用户ID（userID）
    var userIdColumn = "userid"
    //    - 频道ID（channelid）
    var channelidColumn = "channelid"
    //    - 最后访问时间（时间戳）（lastVisitedTime）
    var lastVisitedTimeColumn = "lastVisitedTime"


    var userId: String = HBaseUtil.getData(tableName, rowkey, clfName, userIdColumn)
    var channelid: String = HBaseUtil.getData(tableName, rowkey, clfName, channelidColumn)
    var lastVisitedTime: String = HBaseUtil.getData(tableName, rowkey, clfName, lastVisitedTimeColumn)


    // 如果 userid 为空，则该用户一定是新用户
    if(StringUtils.isBlank(userId)){
      isNew = 1
      isHourNew = 1
      isDayNew = 1
      isMonthNew = 1

      // 保存用户的访问记录到 "user_history"
      HBaseUtil.putMapData(tableName, rowkey, clfName, Map(
        userIdColumn -> msg.clickLog.userID ,
        channelidColumn -> msg.clickLog.channelID ,
        lastVisitedTimeColumn -> msg.timeStamp
      ))
    }
    else{
      isNew = 0
      // 其它字段需要进行时间戳的比对
      isHourNew = compareDate(msg.timeStamp, lastVisitedTimeColumn.toLong, "yyyyMMddHH")
      isDayNew = compareDate(msg.timeStamp, lastVisitedTimeColumn.toLong, "yyyyMMdd")
      isMonthNew = compareDate(msg.timeStamp, lastVisitedTimeColumn.toLong, "yyyyMM")

      // 更新 "user_history" 用户的时间戳
      HBaseUtil.putData(tableName, rowkey, clfName, lastVisitedTimeColumn , msg.timeStamp.toString)

    }

    (isDayNew, isHourNew, isDayNew, isMonthNew)
  }


  /**
    * 比对时间： 201912 > 201911
    * @param currentTime  当前时间
    * @param historyTime  历史时间
    * @param format       时间格式： yyyyMM yyyyMMdd
    * @return             1 或者 0
    */
  def compareDate(currentTime:Long, historyTime:Long, format:String):Int={

    val currentTimeStr:String = timestamp2Str(currentTime, format)
    val historyTimeStr:String = timestamp2Str(historyTime, format)

    // 比对字符串大小，如果当前时间 > 历史时间，返回1
    var result:Int = currentTimeStr.compareTo(historyTimeStr)

    if(result > 0){
      result = 1
    }
    else {
      result = 0
    }
    result
  }

  /**
    * 转换日期
    * @param timestamp  Long 类型时间戳
    * @param format     日期格式
    * @return
    */
  def timestamp2Str(timestamp:Long, format:String):String={
    FastDateFormat.getInstance("yyyyMM").format(timestamp)
  }



}
