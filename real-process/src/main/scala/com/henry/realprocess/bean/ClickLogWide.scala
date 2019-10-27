package com.henry.realprocess.bean

/**
  * @Author: Henry
  * @Description:
  * @Date: Create in 2019/10/27 14:19 
  **/

//  频道ID（channelID）
//  产品类别ID（categoryID）
//  产品ID（produceID）
//  国家（country）
//  省份（province）
//  城市（city）
//  网络方式（network）
//  来源方式（source）
//  浏览器类型（browserType）
//  进入网站时间（entryTime）
//  离开网站时间（leaveTime）
//  用户ID（userID）
// ----  添加以下字段 ---------------
//    用户访问次数（count）
//    用户访问的时间（timestamp）
//    国家省份城市（拼接）（address）
//    年月（yearMonth）
//    年月日（yearMonthDay）
//    年月日时（yearMonthDayHour）
//    是否为访问某个频道的新用户（isNew）—— 0：表示否； 1：表示是
//    在某一小时内是否为某个频道的新用户（isHourNew）—— 0：表示否； 1：表示是
//    在某一天内是否为某个频道的新用户（isDayNew）—— 0：表示否； 1：表示是
//    在某一天月是否为某个频道的新用户（isMonthNew）—— 0：表示否； 1：表示是

case class ClickLogWide (
        // 1、alt + 下拉
        // 2、ctrl + shift + →，选中各个变量
          var channelID:String,
          var categoryID:String,
          var produceID:String,
          var country:String,
          var province:String,
          var city:String,
          var network:String,
          var source:String,
          var browserType:String,
          var entryTime:String,
          var leaveTime:String,
          var userID:String,
  //---  新增 ---------------------------
          var count:Long,
          var timestamp:Long,
          var address:String,
          var yearMonth:String,
          var yearMonthDay:String,
          var yearMonthDayHour:String,
          var isNew:Int,
          var isHourNew:Int,
          var isDayNew:Int,
          var isMonthNew:Int

)

