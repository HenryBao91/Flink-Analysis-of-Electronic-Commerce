package com.henry.syncdb.task

import java.util

import com.alibaba.fastjson.JSON
import com.henry.syncdb.bean.{Cannal, HBaseOperation}
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.api.scala._

import scala.collection.JavaConverters._
import scala.collection.mutable

case class NameValuePair(
                          var columnName: String,
                          var columnValue: String,
                          var isValid: Boolean
                        )

object PreprocessTask {

  def process(canalDataStream: DataStream[Cannal]) = {

    // flatmap

    canalDataStream.flatMap {
      canal => {

        // 把canal.columnValueList转换为scala的集合
//        JSON.parseArray 转换之后是一个java集合
        val javaList: util.List[NameValuePair] = JSON.parseArray(canal.columnValueList, classOf[NameValuePair])
        val nameValueList: mutable.Buffer[NameValuePair] = javaList.asScala

        // HBaseOpertation相关字段
        var opType = canal.eventType
        val tableName = "mysql." + canal.dbName + "." + canal.tableName
        val cfName = "info"
        val rowkey = nameValueList(0).columnValue

        // 遍历集合,先判断是insert还是update或者delete
        opType match {
          case "INSERT" =>
            nameValueList.map {
              nameValue => HBaseOperation(opType, tableName, cfName, rowkey, nameValue.columnName, nameValue.columnValue)
            }

          case "UPDATE" =>
            nameValueList.filter(_.isValid).map {
              nameValue => HBaseOperation(opType, tableName, cfName, rowkey, nameValue.columnName, nameValue.columnValue)
            }

          case "DELETE" =>
            List(HBaseOperation(opType,tableName,cfName,rowkey,"",""))

        }

//        List[HBaseOperation]()
      }
    }

  }

}
