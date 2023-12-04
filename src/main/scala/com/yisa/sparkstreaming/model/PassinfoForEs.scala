package com.yisa.sparkstreaming.model

/**
* @author gaoxl
* @date  2017年4月7日 
* 
*/
case class PassInfoForEs (

  //var id: String,
  var A001: String,
  
//  var locationId: String, // 卡口编号
  var A002: String,
  
//  var levelId: Int, // 车的级别：suv 越野 mpv等
  var A003: Int,
  
//  var yearId: Int ,// 年款
  var A004: Int,
  
//  var modelId: Int ,// 车型
  var A005: Int,
  
//  var brandId :Int=1,
  var A006: Int,
  
//  var plateNumber: String, // 车牌
  var A007: String,
  
//  var regionCode: Int, // 行政区划编码
  var A008: Int,
  
//  var captureTime: Long = 0, // 抓拍时间
  var A009: Long,
  
//  var partType: Int, // 1:车头,2:车尾,-1:未识别
  var A011: Int,
  
//  var colorId: Int, // 颜色
  var A012: Int,
  
//  var directionId: String ,// 方向
  var A013: String,
  
//  var plateTypeId: Int ,// 号牌种类:1:蓝牌，2：黄牌
  var A014: Int,
  
//  var isSheltered: Int ,// 是否遮挡面部
  var A015: Int,
  
//  var fakeplates: Int, // 套牌车假牌车
  var A016: Int,
  
//  var lastCaptured: Int, // 距离上次入城时间
  var A017: Int,
  
//  var jiujia: Int, // 是有否酒驾记录
  var A018: Int,
  
//  var xidu: Int, // 是否有吸毒记录
  var A019: Int,
  
//  var zuijia: Int,   // 是否有醉驾记录
  var A020: Int,
  
//  var zaitao: Int, // 是否是在逃人员
  var A021: Int,
  
//  var zhongdian: Int, // 是否是重点人员
  var A022: Int,
  
//  var shean: Int, // 是否为涉案人员
  var A023: Int,
  
//  var wuzheng: Int, // 是否是无证驾驶
  var A024: Int,
  
//  var yuqi: Int, // 是否为逾期未报废
  var A025: Int,
  
//  var gaoweidiqu: Int, // 高危地区
  var A026: Int,
  
//  var teshugaowei: Int, // 特殊高危地区
  var A027: Int,
  
//  var weizhang: Int, // 是否为违章未处理
  var A040: Int,
  
//  var createTime: Long, // 数据入库时间
  var S002: Long
  
  // 目前没有做！GPU_WF
  
////  var dangerous_chemicals_id : Int,  // 值【0，1】，分别为不是危化品车辆、是危化品车辆
//  var A046: Int,
//  
////  var ctruck_lid_id_ : Int // 值【0，1】，分别为渣土车盖了盖子、是渣土车没盖盖子
//  var A047: Int,
//  
//  //kafka字段不明  target_type 值【1,2,3,4】，1保留，2两轮车，3三轮车，4汽车
//  var A048: Int,
//  
////  var sendTime : Long // 抓拍时间
//  var A051: Long,
//  
////  var construction_truck_id_ : Int,  // 值【0，1】，分别为不是渣土车辆、是渣土车辆
//  var A032: Int,
//  
////  var manned_id_ : Int, // 值【0，1】，分别为没有载人、有载人行为
//  var A030: Int,
//  
//  // 是否逾期未年检
//  var A028: Int

)