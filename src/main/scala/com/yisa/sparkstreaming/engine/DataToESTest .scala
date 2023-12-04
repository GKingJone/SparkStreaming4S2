package com.yisa.sparkstreaming.engine

import java.text.SimpleDateFormat
import java.util.Date

import scala.util.Random

import org.apache.commons.cli.CommandLine
import org.apache.commons.cli.MissingOptionException
import org.apache.commons.cli.Option
import org.apache.commons.cli.Options
import org.apache.commons.cli.PosixParser
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext

import com.google.gson.Gson
import com.google.gson.reflect.TypeToken
import com.yisa.sparkstreaming.manager.KafkaManager
import com.yisa.sparkstreaming.model.PassInfo2
import com.yisa.sparkstreaming.source.Config10

import kafka.serializer.StringDecoder
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import com.yisa.sparkstreaming.model.PassinfoForLast
import com.yisa.sparkstreaming.source.HBaseConnectManager
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.Cell
import scala.beans.BeanProperty
import org.elasticsearch.spark.rdd.EsSpark
import org.elasticsearch.hadoop.cfg.ConfigurationOptions
import com.yisa.sparkstreaming.source.ESClient
import org.elasticsearch.action.bulk.BulkRequestBuilder
import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.search.SearchHits
import org.elasticsearch.search.SearchHit
import org.elasticsearch.action.search.SearchType
import org.elasticsearch.search.sort.SortOrder

/**
 * 低级Api实现
 *
 * ES写入
 */
object DataToESTest {

  def main(args: Array[String]) {
    //文件路径  
    //创建本地的spark配置，此处为本机测试，若在spark集群中需要设置集群  
    val conf = new SparkConf().setAppName("DataToES").setMaster("local")
    conf.set("es.index.auto.create", "true")
    conf.set(ConfigurationOptions.ES_NODES, "192.168.48.128") //节点信息  
    val sc = new SparkContext(conf)
    //
    var info = Array("{\"name\":\"SparkToEs666\",\"age\":68}","{\"name\":\"SparkToEs666\",\"age\":70}","{\"name\":\"SparkToEs666\",\"age\":75}")
    println("------------------------------------------------")
    EsSpark.saveJsonToEs(sc.parallelize(info), "gaoxl4/info")
    println("------------------------------------------------")
    var json = "{" + "\"user\":\"kimchy\"," + "\"postDate\":\"2013-01-30\",\"message\":\"trying out Elasticsearch\"" + "}";
//    ESClient.getInstance("", "192.168.48.128").prepareIndex("gaoxl3", "info", "AVt8uhGX5TTumatTGH-t").setSource("{\"name\":\"SparkToEs555\",\"age\":55}").get()
//    ESClient.getInstance("", "192.168.48.128").prepareIndex("gaoxl4", "info", "AVt8uhGX5TTumatTGH-t").setSource("{\"name\":\"SparkToEs666\",\"age\":66}").get();
//    ESClient.getInstance("", "192.168.48.128").prepareUpdate("gaoxl", "info", "AVt8uhGX5TTumatTGH-t").setDoc("{\"name\":\"SparkToEs12\"}").get();

    var client = ESClient.getInstance("", "192.168.48.128")
    var bulkRequest: BulkRequestBuilder = client.prepareBulk()
    bulkRequest.add(client.prepareUpdate("gaoxl4", "info", "AVt8uhGX5TTumatTGH-t").setDoc("{\"age\":\"77\"}"))
    bulkRequest.execute().actionGet()
    
    
    var queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.termQuery("name.keyword", "SparkToEs666"))
    
    var rangeBuilder = QueryBuilders.rangeQuery("age").from(67).to(78)
    
    var searchResponse = client.prepareSearch("gaoxl4").setTypes("info")
    .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
        .setQuery(queryBuilder)
        .setPostFilter(rangeBuilder)
        .addSort("age", SortOrder.ASC)
        .setSize(1)
        .execute()
        .actionGet()
        
        var hits = searchResponse.getHits
        System.out.println("查询到记录数=" + hits.getTotalHits());
    
        var searchHists = hits.getHits
        if(searchHists.length>0){
          var a = 0
            for(a <- 0 until searchHists.length){
              var hit = searchHists(a)
              var m = hit.getSource()
              println(hit.getId)
                var id = m.get("age")
                println(id)
            }
        }
    client.close()
    sc.stop()
  }
}

case class DataBean(@BeanProperty var name: String,
                    @BeanProperty var age: Int)