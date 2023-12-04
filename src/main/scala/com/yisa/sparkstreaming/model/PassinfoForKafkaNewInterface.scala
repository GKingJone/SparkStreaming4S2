package com.yisa.sparkstreaming.model

/**
* @author liliwei
* @date  2016年9月20日 
* 
*/
case class PassInfoForKafkaNewInterface (

  // static final long serialVersionUID = 8610339732673791351L,

  var id: String,
  var locationuuid : String,
  var levelId : Int,
  var yearId : Int,
  var modelId : Int,
  var brandId :Int,
  var plateNumber : String,
  var regionCode : Int,
  var captureTime : Long,
  var nodeId : String,
  var partType : Int,
  var colorId : Int,
  var directionId : String,
  var plateTypeId : Int,
  var plateCategoryId : Int,
  var lastCaptured : Long,
  var skylight : Int,
  var baggageHold : Int,
  var sprayWord : Int,
  var inspectionTag : Int,
  var sunShadeLeft : Int,
  var sunShadeRight : Int,
  var pendant : Int,
  var tissueBox : Int,
  var decoration : Int,
  var card : Int,
  var personLeft : Int,
  var personRight : Int,
  var backBurnerTail : Int,
  var sendTime : Long,
  var recPlateNumber : String,
  var locationId : String,
  var carBox : String,
  var imageUrl : String,
  var speed : Int,
  var feature : String,
  var yearIdProb : Float

)