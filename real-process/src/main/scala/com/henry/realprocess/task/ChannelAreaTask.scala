package com.henry.realprocess.task
import com.henry.realprocess.bean.ClickLogWide
import com.henry.realprocess.util.HBaseUtil
import org.apache.commons.lang.StringUtils
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, WindowedStream}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time


/**
  * @Author: Henry
  * @Description:
  * @Date: Create in 2019/11/3 14:06 
  **/

// 样例类
case class ChannelArea(
                        var channelId: String,
                        var area: String,
                        var date: String,
                        var pv: Long,
                        var uv: Long,
                        var newCount: Long,
                        var oldCount: Long
                      )

object ChannelAreaTask extends BaseTask [ChannelArea]{

  // 1、 转换
  override def map(clickLogWideDataStream: DataStream[ClickLogWide]): DataStream[ChannelArea] = {

    clickLogWideDataStream.flatMap{

      clickLogWide =>{

        // 如果是老用户，并且在该时间段内第一次来，就计数 1. 否则计 0
        val isOld = (isNew: Int, isDateNew: Int) => if (isNew == 0 && isDateNew == 1) 1 else 0

        List(
            ChannelArea(      //  月维度
                clickLogWide.channelID,
                clickLogWide.address,
                clickLogWide.yearMonth,
                clickLogWide.count,         //  pv， 每来一个数据进行累加
                clickLogWide.isMonthNew,    //  uv， 第一次来的时候只计数一次
                clickLogWide.isNew,         //  当是 New 的时候进行累加
                isOld(clickLogWide.isNew, clickLogWide.isMonthNew)
            ),
            ChannelArea(      //  日维度
              clickLogWide.channelID,
              clickLogWide.address,
              clickLogWide.yearMonth,
              clickLogWide.count,
              clickLogWide.isDayNew,
              clickLogWide.isNew,
              isOld(clickLogWide.isNew, clickLogWide.isDayNew)
            ),
            ChannelArea(      //  小时维度
              clickLogWide.channelID,
              clickLogWide.address,
              clickLogWide.yearMonth,
              clickLogWide.count,
              clickLogWide.isHourNew,
              clickLogWide.isNew,
              isOld(clickLogWide.isNew, clickLogWide.isHourNew)
            )
        )
      }
    }
  }

  // 2、 分组 根据 频道ID+地域+时间
  override def keyBy(mapDataStream: DataStream[ChannelArea]): KeyedStream[ChannelArea, String] = {
    mapDataStream.keyBy{
      area =>
        area.channelId + " : " + area.area + " : " + area.date
    }
  }

  // 3、 时间窗口, 这段代码每个子类都是一样的,可以写到父类中
//  override def timeWindow(keyedStream: KeyedStream[ChannelArea, String]): WindowedStream[ChannelArea, String, TimeWindow] = {}


  // 4、 聚合 累加4个字段
  override def reduce(windowedStream: WindowedStream[ChannelArea, String, TimeWindow]) = {
    windowedStream.reduce {
      (t1, t2) =>
        ChannelArea(t1.channelId, t1.area,
          t1.date,
          t1.pv + t2.pv,
          t1.uv + t2.uv,
          t1.newCount + t2.newCount,
          t1.oldCount + t2.oldCount)
    }
  }


  // 5、 落地HBase
  override def sink2HBase(reduceDataStream: DataStream[ChannelArea]): Unit = {
    reduceDataStream.addSink{
      area => {
        // HBase 相关字段
        val tableName = "channel_area"
        val clfName = "info"
        val rowkey = area.channelId + ":" + area.area + ":" + area.date

        val channelIdColumn = "channelId"
        val areaColumn = "area"
        val dateColumn = "date"
        val pvColumn = "pv"
        val uvColumn = "uv"
        val newCountColumn = "newCount"
        val oldCountColumn = "oldCount"

        //  查询 HBase
        val pvInHbase: String = HBaseUtil.getData(tableName,rowkey,clfName,pvColumn)
        val uvInHbase: String = HBaseUtil.getData(tableName,rowkey,clfName,uvColumn)
        val newCountInHbase: String = HBaseUtil.getData(tableName,rowkey,clfName,newCountColumn)
        val oldCountInHbase: String = HBaseUtil.getData(tableName,rowkey,clfName,oldCountColumn)

        // 累加
        var totalPv = 0L
        var totalUv = 0L
        var totalNewCount = 0L
        var totalOldCount = 0L

        // PV
        if(StringUtils.isNotBlank(pvInHbase)){
          totalPv  = pvInHbase.toLong+area.pv
        }else{
          totalPv = area.pv
        }

        // UV
        if(StringUtils.isNotBlank(uvInHbase)){
          totalUv  = uvInHbase.toLong+area.uv
        }else{
          totalUv = area.uv
        }

        // totalNewCount
        if(StringUtils.isNotBlank(newCountInHbase)){
          totalNewCount  = newCountInHbase.toLong+area.newCount
        }else{
          totalNewCount = area.newCount
        }

        // totalOldCount
        if(StringUtils.isNotBlank(oldCountInHbase)){
          totalOldCount  = oldCountInHbase.toLong+area.oldCount
        }else{
          totalOldCount = area.oldCount
        }

        //  保存数据
        HBaseUtil.putMapData(tableName,rowkey,clfName,Map(
          channelIdColumn->area.channelId,
          areaColumn->area.area,
          dateColumn->area.date,
          pvColumn->totalPv,
          uvColumn->totalUv,
          newCountColumn->totalNewCount,
          oldCountColumn->totalOldCount
        ))

      }
    }
  }
}
