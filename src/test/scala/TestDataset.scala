

import scala.collection.mutable.ArrayBuffer

object TestDataset {
  
  case class Person(age : Int, name : String)
  
  var map: ArrayBuffer[Person] =new  ArrayBuffer[Person]
  
  def main(args : Array[String]){
    
          import org.apache.spark.sql.SparkSession

          val spark = SparkSession
            .builder()
            .appName("Spark SQL Example")
            .master("local")
            .config("spark.sql.warehouse.dir", "file:///D:/Workspace/spark/spark-warehouse")
            .getOrCreate()

          // For implicit conversions like converting RDDs to DataFrames
          import spark.implicits._

          var person = new Person(44, "gao")
          var person1 = new Person(44, "gao")
          map.+=(person)
          map.+=(person1)
          val aa = Seq(Array(2, 3, 5, 6)).toDS()
          aa.createOrReplaceTempView("name")
          aa.show();
          
          Array(2, 3, 5, 6)
          
          spark.sql("select * from name")
  }
}