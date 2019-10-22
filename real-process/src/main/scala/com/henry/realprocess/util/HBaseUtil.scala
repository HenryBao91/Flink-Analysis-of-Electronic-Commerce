package com.henry.realprocess.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ColumnFamilyDescriptor, _}

/**
  * @Author: Henry
  * @Description: HBase 工具类
  *              1、获取Table对象
  *              2、保存单列数据
  *              3、查询单列数据
  *              4、保存多列数据
  *              5、查询多列数据
  *              6、删除数据
  * @Date: Create in 2019/10/21 22:53 
  **/
object HBaseUtil {

  // HBase 配置类，不需要指定配置文件名，文件名要求是 hbase-site.xml
  val conf:Configuration = HBaseConfiguration.create()

  // HBase 的连接
  val conn:Connection = ConnectionFactory.createConnection(conf)

  // HBase 的操作 API
  val admin:Admin = conn.getAdmin

  /**
    *  返回Table，如果不存在，则创建表
    *
    * @param tableName
    * @param columnFamilyName
    * @return
    */
  def getTable(tableNameStr:String, columnFamilyName:String):Table={


    // 获取 TableName
    val tableName:TableName = TableName.valueOf(tableNameStr)

    // 如果表不存在，则创建表

    if(!admin.tableExists(tableName)){

      // 构建出表的描述的建造者
      val descBuilder: TableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableName)

      val familyDescriptor:ColumnFamilyDescriptor = ColumnFamilyDescriptorBuilder
        .newBuilder(columnFamilyName.getBytes).build()

      // 给表添加列族
      descBuilder.setColumnFamily(familyDescriptor)

      // 创建表
      admin.createTable(descBuilder.build())
    }

    conn.getTable(tableName)

  }

  def main(args: Array[String]): Unit = {

    println(getTable("test","nfo"))
  }

}
