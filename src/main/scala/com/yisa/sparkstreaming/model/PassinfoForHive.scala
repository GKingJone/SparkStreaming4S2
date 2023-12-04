package com.yisa.sparkstreaming.model

/**
* @author liliwei
* @date  2016年9月20日 
* 
*/
import java.sql.Timestamp


   case class PassinfoForHive(
    var solrid: String,
    var platenumber: String,
    var capturetime: Timestamp,
    var directionid: String,
    var colorid: Int,
    var modelid: Int,
    var brandid: Int,
    var levelid: Int,
    var yearid: Int,
    var recfeature: String,
    var dateid: Int,
    var locationid: String)

