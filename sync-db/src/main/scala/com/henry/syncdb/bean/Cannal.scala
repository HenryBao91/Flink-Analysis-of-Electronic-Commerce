package com.henry.syncdb.bean

import com.alibaba.fastjson.JSON

/**
  * @Author: Henry
  * @Description:
  * @Date: Create in 2019/11/7 19:28 
  **/
case class Cannal(
                 var emptyCount:Long,
                 var logFileName:String,
                 var dbName:String,
                 var logFileOffset:Long,
                 var eventType:String,
                 var columnValueList:String,
                 var tableName:String,
                 var timestamp:Long
                )

object Cannal {

  def apply(json:String): Cannal = {
    val canal: Cannal = JSON.parseObject[Cannal](json,classOf[Cannal])
    canal
  }

  def main(args: Array[String]): Unit = {

    val json = "{\"emptyCount\":2,\"logFileName\":\"mysql-bin.000005\",\"dbName\":\"pyg\",\"logFileOffset\":20544,\"eventType\":\"INSERT\",\"columnValueList\":[{\"columnName\":\"commodityId\",\"columnValue\":\"6\",\"isValid\":true},{\"columnName\":\"commodityName\",\"columnValue\":\"欧派\",\"isValid\":true},{\"columnName\":\"commodityTypeId\",\"columnValue\":\"3\",\"isValid\":true},{\"columnName\":\"originalPrice\",\"columnValue\":\"43000.0\",\"isValid\":true},{\"columnName\":\"activityPrice\",\"columnValue\":\"40000.0\",\"isValid\":true}],\"tableName\":\"commodity\",\"timestamp\":1558764495000}"
    val cannal = Cannal(json)


    println(cannal.timestamp)
    println(Cannal(json).dbName)

  }
}