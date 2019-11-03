package com.henry.realprocess.task

import com.henry.realprocess.bean.ClickLogWide
import com.henry.realprocess.task.ChannelBrowserTask.pvColName
import org.apache.commons.lang.StringUtils
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, WindowedStream}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

/**
  * @Author: Henry
  * @Description:
  * @Date: Create in 2019/11/3 10:42 
  **/

trait BaseTask[T] {


  // 1、 转换
  def map(clickLogWideDataStream : DataStream[ClickLogWide]): DataStream[T]

  //     2、 分组
  def keyBy(mapDataStream : DataStream[T]): KeyedStream[T, String]

  // 3、 时间窗口
  def timeWindow(keyedStream: KeyedStream[T, String]) : WindowedStream[T, String, TimeWindow] = {
    // 因为所有自类都是 3 秒的时间窗口
      keyedStream.timeWindow(Time.seconds(3))
  }

  // 4、 聚合
  def reduce(windowedStream : WindowedStream[T, String, TimeWindow]) : DataStream[T]

  // 5、 落地 HBase
  def sink2HBase(reduceDataStream: DataStream[T])


  // 定义模板执行顺序
  def process(clickLogWideDataStream : DataStream[ClickLogWide]): Unit = {
    val mapDataStream: DataStream[T] = map(clickLogWideDataStream)
    val keyedStream: KeyedStream[T, String] = keyBy(mapDataStream)
    val windowedStream: WindowedStream[T, String, TimeWindow] = timeWindow(keyedStream)
    val reduceStream: DataStream[T] = reduce(windowedStream)
    sink2HBase(reduceStream)
  }

  //  检测老用户是否第一次访问
  val isOld = (isNew: Int, isDateNew: Int) => if (isNew == 0 && isDateNew == 1) 1 else 0

  // 创建 HBase 相关列
  var tableName = ""
  var clfName = "info"
  var rowkey = ""
  var channelIdColName = "channelID"
  var browserColName = "browser"
  var dateColName = "date"
  var pvColName = "pv"
  var uvColName = "uv"
  var newCountColName = "newCount"
  var oldCountColName = "oldCount"


  /* 累加相关列的值
  * @param resultMap    map集合
  * @param column       待查询的列
  * @param currentValue 当前值
  * @return             累加后的值
  */
  def getTotal(resultMap: Map[String, String],column:String,currentValue:Long):Long={

    var total = currentValue
    // 如果resultMap不为空,并且可以去到相关列的值,那么就进行累加
    if (resultMap != null && StringUtils.isNotBlank(resultMap.getOrElse(column,""))) {
      total = resultMap(column).toLong + currentValue
    }
    total
  }


}
