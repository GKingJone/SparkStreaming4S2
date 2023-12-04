package com.yisa.sparkstreaming.model

/**
* @author liliwei
* @date  2016年9月20日 
* 
*/
import java.sql.Timestamp


   case class PassinfoForLast(
    var id : String,
    var platenumber : String,
    var capturetime: Long,
    var lastCaptured: Long)

