package com.yisa.sparkstreaming.model

/**
* @author liliwei
* @date  2016年9月20日 
* 
*/
case class PassInfo2 (

  // static final long serialVersionUID = 8610339732673791351L,

  var id: String,
  var nodeId: String, // 节点id
  var sourceId: Int = 3 ,// 数据来源0：电警 1卡口，3，不确定
  var plateNumber: String, // 车牌
  var plateTypeId: Int ,// 号牌种类:1:蓝牌，2：黄牌
  var locationId: String, // 卡口编号
  var deviceId: String, // 设备编号
  var laneId: Int ,// 车道编号
  var speed: Int, // 车辆速度
  var captureTime: Long = 0, // 抓拍时间
  var passTime: String ,// 抓拍时间(字符串类型：用于接收数据，不进行存储)
  var directionId: String ,// 方向
  var colorId: Int, // 颜色
  var modelId: Int ,// 车型
  var imageUrl: String ,// 图片地址
  var isSheltered: Int ,// 是否遮挡面部
  var isSafebelt: Int, // 是否未系安全带
  var isPhoneface: Int, // 是否打电话
  var locationuuid: String,
  var countyId: Int,
  var levelId: Int, // 车的级别：suv 越野 mpv等
  var lastCaptured: Int, // 距离上次入城时间
  var lastCapturedId: String,
  var brandId :Int=1,
  var partType: Int, // 1:车头,2:车尾,-1:未识别
  var yearId: Int ,// 年款
  var coordinates: String ,// 纬度,经度
  var regionCode: Int, // 行政区划编码
  var hasTag: Int, // 有无年检标志，后期变为年检标志数量
  var hasZjh: Int, // 有无纸巾盒
  var hasZsw: Int, // 有无装饰物
  var remoteImageUrl: String, // 原图在FDFS存储的路径
  var checksum: String, // #此哈希值对应mysql数据库pass_info表中checksum字段，根据checksum字段获取image_uuid_1，拼接出图片地址
  var recYearId: String, // 图像识别接口识别出来的车型（以英文逗号拼接的5个车型ID）
  var carBox: String, // 车辆图片坐标信息
  var recPlateNumber: String, // 图像识别接口识别出来的车牌号
  var feature: String, // 车特征，以图搜车
  var fakeplates: Int, // 套牌车假牌车
  var parttype: Int, // 前端使用，我们不使用
  // 登记的品牌
  var realBrandId: String,
  // vin码
  var vinCode: String,

  // 数据入库时间
  var createTime: Long,

  // 这个是用来给php版神眼用的
  var phpCarBox: String,

  // 用来标记数据来源
  var dataSource: String,

  // car_year中的groupID字段，用来判断是否是高仿车辆，如众泰
  var groupId: Int,

  // 是否有吸毒记录
  var xidu: Int,

  // 是有否酒驾记录
  var jiujia: Int,
  // 是否有醉驾记录
  var zuijia: Int,
  // 是否是在逃人员
  var zaitao: Int,
  // 是否是重点人员
  var zhongdian: Int,
  // 是否为涉案人员
  var shean: Int,
  // 是否是无证驾驶
  var wuzheng: Int,
  // 是否为违章未处理
  var weizhang: Int,
  // 是否为逾期未报废
  var yuqi: Int,
  // 高危地区
  var gaoweidiqu: Int,
  var teshugaowei: Int,
  // 主驾安全带 值【0，1】，分别为右侧驾驶员系安全带、未系安全带;
  var right_nobelt_id:Int,
  // 副驾安全带 值【0，1】，分别为左侧驾驶员系安全带、未系安全带
  var left_nobelt_id: Int,
  //主驾电话 值【0，1】，分别为右侧驾驶员没有打电话、有打电话
  var right_phone_id: Int,
  // 主驾图片 右侧驾驶员图片 格式为base64编码
  var right_img: String,
  // 副驾图片 左侧驾驶员图片 格式为base64编码
  var left_img: String,
  // 值【0，1】，分别为没有载人、有载人行为
  var manned_id_ : Int,
  // 值【0，1】，分别为不是危化品车辆、是危化品车辆
  var dangerous_chemicals_id : Int,
  // 值【0，1】，分别为不是渣土车辆、是渣土车辆
  var construction_truck_id_ : Int,
  // 值【0，1】，分别为渣土车盖了盖子、是渣土车没盖盖子
  var ctruck_lid_id_ : Int,
  // 抓拍时间
  var sendTime : Long,
  
  // 新增
  var plateCategoryId : Int

)