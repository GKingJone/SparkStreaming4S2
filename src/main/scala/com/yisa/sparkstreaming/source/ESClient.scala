package com.yisa.sparkstreaming.source

import org.apache.spark.sql.SparkSession
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.transport.client.PreBuiltTransportClient
import org.elasticsearch.common.transport.TransportAddress
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.common.network.InetAddresses

object ESClient extends Serializable {

  @transient private var instance: TransportClient = _

  def getInstance(clusterName: String, servers: String): TransportClient = {

    if (instance == null) {
      synchronized {
        if (instance == null) {

          try {
            // 自动把集群下的机器添加到列表中
            var settings: Settings = Settings.builder().put("cluster.name", "wuhanes").put("client.transport.sniff", true).build();

            instance = new PreBuiltTransportClient(settings);

            //添加集群IP列表
            if (servers.contains(",")) {
              
              servers.split(",").foreach { ip =>
                var transportAddress : TransportAddress = new InetSocketTransportAddress(InetAddresses.forString(ip), 9300);
                instance.addTransportAddresses(transportAddress);
              }
              
            } else {
              
                var transportAddress : TransportAddress = new InetSocketTransportAddress(InetAddresses.forString(servers), 9300);
                instance.addTransportAddresses(transportAddress);
              
            }
            
          } catch {
            case ex: Exception => {
              println("ESClient初期化失败！")
              if(instance != null){
                instance.close()
              }
            }
          }

        }

      }

    }

    instance

  }
}