package com.yisa.sparkstreaming.engine


import org.apache.commons.cli.CommandLine
import org.apache.commons.cli.Options
import org.apache.commons.cli.Option

object DataToESqxd {
  def main(args: Array[String]){
    var hbaseTableName: String = "last_captured1"
    var cmd: CommandLine = null
    val option:Options =new Options()
    
    try {
      var zkServer: Option =new Option("zk_server",true,"输入zookeeper服务器地址")
    } catch {
      case t: Throwable => t.printStackTrace() // TODO: handle error
    }    
    
  }
}