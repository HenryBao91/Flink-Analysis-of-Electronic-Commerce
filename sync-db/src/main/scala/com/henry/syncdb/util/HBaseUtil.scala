package com.henry.syncdb.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{ColumnFamilyDescriptor, _}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}

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
    * @param tableName        表名
    * @param columnFamilyName 列族名
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

  /**
    * 存储单列数据
    *
    * @param tableNameStr     表名
    * @param rowkey           主键
    * @param columnFamilyName 列族名
    * @param columnName       列名
    * @param columnValue      列值
    */
  def putData(tableNameStr:String, rowkey:String, columnFamilyName:String, columnName:String, columnValue:String)={

    // 获取表
    val table:Table = getTable(tableNameStr, columnFamilyName)

    try{
      // Put
      val put:Put = new Put(rowkey.getBytes)
      put.addColumn(columnFamilyName.getBytes, columnName.getBytes, columnValue.getBytes)

      // 保存数据
      table.put(put)
    }catch {
      case ex:Exception=>{
        ex.printStackTrace()
      }
    }finally {
      table.close()
    }
  }


  /**
    * 通过单列名获取列值
    * @param tableNameStr     表名
    * @param rowkey           主键
    * @param columnFamilyName 列族名
    * @param columnName       列名
    * @param columnValue      列值
    * @return
    */
  def getData(tableNameStr:String, rowkey:String, columnFamilyName:String, columnName:String):String={

    // 1. 获取 Table 对象
    val table = getTable(tableNameStr, columnFamilyName)

    try {
      // 2. 构建 get 对象
      val get = new Get(rowkey.getBytes)

      // 3. 进行查询
      val result:Result = table.get(get)

      // 4. 判断查询结果是否为空，并且包含要查询的列
      if (result != null && result.containsColumn(columnFamilyName.getBytes, columnName.getBytes)){
        val bytes: Array[Byte] = result.getValue(columnFamilyName.getBytes(), columnName.getBytes)

        Bytes.toString(bytes)
      }else{
        ""
      }

    }catch{
      case ex:Exception => {
        ex.printStackTrace()
        ""
      }
    }finally {
      // 5、关闭表
      table.close()
    }

  }


  /**
    * 存储多列数据
    * @param tableNameStr     表名
    * @param rowkey           主键
    * @param columnFamilyName 列族名
    * @param map              多个列名和列族集合
    */
  def putMapData(tableNameStr:String, rowkey:String, columnFamilyName:String, map:Map[String,Any])={

    // 1、获取 table 对象
    val table = getTable(tableNameStr, columnFamilyName)

    try{
      // 2、创建 put
      val put = new Put(rowkey.getBytes)

      // 3、在 put 中添加多个列名和列值
      for ((colName, colValue) <- map){
        put.addColumn(columnFamilyName.getBytes, colName.getBytes, colValue.toString.getBytes)
      }

      // 4、保存 put
      table.put(put)

    }catch{
      case ex:Exception => {
        ex.printStackTrace()

      }
    }finally {
      // 5、关闭表
      table.close()
    }


    // 5、关闭 table
    table.close()
  }


  /**
    * 获取多了数据的值
    * @param tableNameStr     表名
    * @param rowkey           主键
    * @param columnFamilyName 列族名
    * @param columnNameList   多个列名和列值集合
    * @return
    */
  def getMapData(tableNameStr:String, rowkey:String, columnFamilyName:String, columnNameList:List[String]):Map[String,String]= {

    // 1、获取 Table
    val table = getTable(tableNameStr, columnFamilyName)

    try{
      // 2、构建 get
      val get = new Get(rowkey.getBytes)

      // 3、执行查询
      val result: Result = table.get(get)

      // 4、遍历列名集合，取出列值，构建成 Map 返回
      columnNameList.map {
        col =>
          val bytes: Array[Byte] = result.getValue(columnFamilyName.getBytes(), col.getBytes)

          if (bytes != null && bytes.size > 0) {
            col -> Bytes.toString(bytes)
          }
          else {   // 如果取不到值，则赋一个空串
            "" -> ""
          }
      }.filter(_._1 != "").toMap  // 把不是空串的过滤出来，再转换成 Map

    }catch {
      case ex:Exception => {
        ex.printStackTrace()
        Map[String, String]()   // 返回一个空的 Map
      }
    }finally {
      // 5、关闭 Table
      table.close()
    }
  }


  /**
    * 删除数据
    * @param tableNameStr     表名
    * @param rowkey           主键
    * @param columnFamilyName 列族名
    */
  def delete(tableNameStr:String, rowkey:String, columnFamilyName:String)={

    // 1、获取 Table
    val table:Table = getTable(tableNameStr, columnFamilyName)

    try {
      // 2、构建 delete 对象
      val delete: Delete = new Delete(rowkey.getBytes)

      // 3、执行删除
      table.delete(delete)

    }
    catch {
      case ex:Exception =>
        ex.printStackTrace()
    }
    finally {
      // 4、关闭 table
      table.close()
    }

  }


  def main(args: Array[String]): Unit = {

//    println(getTable("test","info"))
//    putData("test", "1", "info", "t1", "hello world")
//    println(getData("test", "1", "info", "t1"))

//    val map = Map(
//      "t2" -> "scala" ,
//      "t3" -> "hive" ,
//      "t4" -> "flink"
//    )
//    putMapData("test", "1", "info", map)

//    println(getMapData("test", "1", "info", List("t1", "t2")))

    delete("test", "1", "info")
    println(getMapData("test", "1", "info", List("t1", "t2")))

  }

}
