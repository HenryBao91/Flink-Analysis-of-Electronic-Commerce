package com.henry.realprocess.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Admin, Connection, ConnectionFactory}

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
}
