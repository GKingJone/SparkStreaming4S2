package com.yisa.sparkstreaming.test

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import java.util.Date
import org.apache.spark.sql.SQLContext
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.kafka._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.DataFrame
import java.util.Base64
import org.apache.spark.broadcast.Broadcast
import scala.reflect.runtime.universe

object SearchImage {

  def main(args: Array[String]) = {

    //  val sprakConf = new SparkConf().setAppName("SparkStreamingTest2").setMaster("local[3]")
    val sprakConf = new SparkConf().setAppName("SearchImageSparkStreaming")
    var sc = new SparkContext(sprakConf)

    val streamingContext = new StreamingContext(sc, Milliseconds(500))
    val kafkaStream = KafkaUtils.createStream(streamingContext, "gpu3:2181/kafka", "test", Map[String, Int]("test" -> 1), StorageLevel.MEMORY_ONLY_SER)

    val sqlContext = new SQLContext(sc)
    import sqlContext._

    var aa = kafkaStream.map(line => {
      if (!line._2.toString().equals("")) {
        println("map:" + line._2)
      }
      line._2
    })

    aa.foreachRDD { rdd =>
      {
        if (rdd.collect().size > 0) {

          val now = new Date().getTime()
          if (!rdd.collect()(0).equals("")) {

            var line = rdd.collect()(0).toString()
            println(line)

            var line_arr = line.split(",")
//            var line_arr = line.substring(1, line.length() - 1).split(",")

            val feature = line_arr(0)
            val modelId = line_arr(1)
            val brandId = line_arr(2)

            val dataFrame = WordBlacklist.getInstance(rdd.sparkContext).value

            dataFrame.createOrReplaceTempView("passinfo")
            sqlContext.udf.register("getSimilarity", (text: String, test2: String) => {
              val byteData = Base64.getDecoder.decode(text)
              val oldData = Base64.getDecoder.decode(test2)
              var num: Long = 0
              for (i <- 0 until byteData.length) {
                var n = (byteData(i) & 0xff) - (oldData(i) & 0xff)
                num += n * n;
              }
              num
            })

            val now2 = new Date().getTime()

            val teenagers = sqlContext.sql("SELECT id,getSimilarity('" + feature + "',recFeature) as similarity FROM passinfo where  recFeature != '' and recFeature != 'null' and brandId = " + brandId + "  and modelId  = " + modelId + " order by similarity  limit 100")

            teenagers.foreachPartition { data =>
              {

                data.foreach { t =>
                  {

                    println("id: " + t(0) + " similarity:" + t(1))
                  }
                }

              }
            }

            val now3 = new Date().getTime()

            val teenagers2 = sqlContext.sql("SELECT id,getSimilarity('" + feature + "',recFeature) as similarity FROM passinfo where  recFeature != '' and recFeature != 'null' and brandId = " + brandId + "  and modelId  = " + modelId + " order by similarity  limit 100")

            teenagers2.foreach { t =>
              {

                println("id: " + t(0) + " similarity:" + t(1))
              }

            }

            //            teenagers2.foreachPartition { data =>
            //              {
            //
            //                data.foreach { t =>
            //                  {
            //
            //                    println("id: " + t(0) + " similarity:" + t(1))
            //                  }
            //                }
            //
            //              }
            //            }

            val now4 = new Date().getTime()

            println("load data:" + (now2 - now))
            println("num:" + (now3 - now2))
            println("num:" + (now4 - now3))
            println("total:" + (now4 - now))

          }
        }
      }
    }
    streamingContext.start()
    streamingContext.awaitTermination()

  }

}

object WordBlacklist {

  @volatile private var instance: Broadcast[DataFrame] = null
  def getInstance(sc: SparkContext): Broadcast[DataFrame] = {
    if (instance == null) {
      synchronized {
        if (instance == null) {
          val sqlContext = new SQLContext(sc)
          import sqlContext._
          import sqlContext.implicits._
          instance = sc.broadcast(sqlContext.read.parquet("hdfs://gpu10:8020/moma/pass_info0824800wan"))
        }
      }
    }
    instance
  }
}