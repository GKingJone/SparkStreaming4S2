

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.text.SimpleDateFormat
import java.sql.Timestamp
import scala.collection.mutable.ArrayBuffer


object Test1 {

  def main(args: Array[String]) = {
    
    test4()
  }
  
  def test4() {
//    println(Config.zookeeper)
  }
  
  def tet3() {
    var sprakConf = new SparkConf().setAppName("test").setMaster("spark://cdh1:7077").set("spark.sql.warehouse.dir", "file:///D:/spark-warehouse")
    var sc = new SparkContext(sprakConf)
    val warehouseLocation = "hdfs://cdh1:8020/user/hive/warehouse"

    val spark = SparkSession
      .builder()
      .appName("Spark Hive Example")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._
    import spark.sql
    
    var map: ArrayBuffer[Person] =new  ArrayBuffer[Person]
    map += Person(44, "jack01")
    val caseClassDS = Seq(map.toArray).toDS()
    caseClassDS.show()
    
    caseClassDS.registerTempTable("tmp_pass_info")
    caseClassDS.printSchema()
    
    sql("SELECT * FROM yisa.parquet_01").show()

    spark.sql("insert into yisa.parquet_01 Select * from tmp_pass_info")

    // Queries are expressed in HiveQL
    sql("SELECT * FROM yisa.parquet_01").show()

  }
  
  def runSpark(){
    
    val spark = SparkSession
      .builder()
      .config("spark.sql.warehouse.dir", "file:///D:/Workspace/spark/spark-warehouse")
      .appName("Spark SQL Example")
      .master("local")
      .getOrCreate()
    
    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._
    
    val df = spark.read.json("examples/src/main/resources/people.json")

    // Displays the content of the DataFrame to stdout
    df.show()
    
    spark.stop()
    
  }

  def getTimestamp(x: String): java.sql.Timestamp = {

    val format = new SimpleDateFormat("yyyyMMddHHmmss")

    try {
      if (x == "")
        return null
      else {
        val d = format.parse(x);
        val t = new Timestamp(d.getTime());
        return t
      }
    } catch {
      case e: Exception => println("cdr parse timestamp wrong")
    }

    return null

  }
  
  case class Person(age : Int, name : String)
}

