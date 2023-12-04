package com.yisa.sparkstreaming.model

/**
* @author liliwei
* @date  2016年9月20日 
* 
*/
import java.sql.Timestamp


   case class PassinfoForHive3(
    var solrid: String,
    var platenumber: String,
    var capturetime: Long,
    var directionid: Int,
    var colorid: Int,
    var modelid: Int,
    var brandid: Int,
    var levelid: Int,
    var yearid: Int,
    var recfeature: String,
    var locationid: String,
    var lastcaptured: Int,
    var issheltered:Int,
    var createtime: Long,
    var regioncode:Int,
    var direction: String,
    var dateid: Int)

