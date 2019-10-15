package com.henry.realprocess.util

import com.typesafe.config.{Config, ConfigFactory}

/**
  * @Author: Henry
  * @Description: 配置文件加载类
  * @Date: Create in 2019/10/15 23:42 
  **/
object GlobalConfigutil {

  // 通过工厂加载配置, config 会自动加载 application.conf 文件，文件名不能变
  val config:Config = ConfigFactory.load()

  val bootstrapServers = config.getString("bootstrap.servers")
  val zookeeperConnect = config.getString("zookeeper.connect")
  val inputTopic = config.getString("input.topic")
  val gruopId = config.getString("gruop.id")
  val enableAutoCommit = config.getString("enable.auto.commit")
  val autoCommitIntervalMs = config.getString("auto.commit.interval.ms")
  val autoOffsetReset = config.getString("auto.offset.reset")

  def main(args: Array[String]): Unit = {
    // 选择快捷键，alt，鼠标左键拉倒最后一行，然后按 ctrl+shift 键，再按 →
    println(bootstrapServers)
    println(zookeeperConnect)
    println(inputTopic)
    println(gruopId)
    println(enableAutoCommit)
    println(autoCommitIntervalMs)
    println(autoOffsetReset)
  }
}
