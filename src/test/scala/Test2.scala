import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.kafka._
import org.apache.spark.sql.SparkSession
import collection.mutable.ArrayBuffer
import org.apache.spark.sql.SparkSession
object Test2 {

  def main(args: Array[String]) = {

    //	  var sprakConf = new SparkConf().setAppName("test").setMaster("spark://cdh1:7077").set("spark.sql.warehouse.dir", "file:///D:/spark-warehouse")
      var sprakConf = new SparkConf().setAppName("test").setMaster("local").set("spark.sql.warehouse.dir", "file:///D:/spark-warehouse")
    //  var sprakConf = new SparkConf().setAppName("test")
    var sc = new SparkContext(sprakConf)

         var map: ArrayBuffer[Person] = new ArrayBuffer[Person]
      
      var person1 = Person("Andy", 32);
    var person2 = Person("Li", 12);
      

          val spark = SparkSession
            .builder()
            .appName("Spark SQL Example")
            .getOrCreate()

          // For implicit conversions like converting RDDs to DataFrames
          import spark.implicits._
  
//        val caseClassDS = Seq(Person("Andy", 32)).toDS()
           var test0 =map.toSeq
     test0= test0.+:(person1)
      test0=  test0.+:(person2)
       
      
      println(test0.size)
        val caseClassDS = test0.toDS()
        caseClassDS.show()
        




  }
case class Person(name: String, age: Long)
 
}

