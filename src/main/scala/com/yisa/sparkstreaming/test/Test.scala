package com.yisa.sparkstreaming.test

import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.Cell
import util.control.Breaks._
import scala.collection.mutable.HashMap
import java.util.regex.Pattern
import java.text.SimpleDateFormat
import scala.collection.mutable.ArrayBuffer
import java.util.Date
import com.yisa.sparkstreaming.source.HBaseConnectManager
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.ConnectionFactory

object Test {

  def main(args: Array[String]): Unit = {

    var tableName = args(0).toString()
    var rowkey = args(1).toString().reverse
    val configuration = HBaseConfiguration.create();

    configuration.set("hbase.zookeeper.property.clientPort", "2181");
    configuration.set("hbase.zookeeper.quorum", "wh2");
    try {
      val hbaseConn = ConnectionFactory.createConnection(configuration)
      val table = hbaseConn.getTable(TableName.valueOf(tableName));
      val get = new Get(Bytes.toBytes(rowkey));
      get.setMaxVersions();
      get.addFamily(Bytes.toBytes("info"))
      //Column(Bytes.toBytes("info"), Bytes.toBytes("yearids"));
      val result = table.get(get);

      var cellsList = result.listCells()

      for (i <- 0 until cellsList.size()) {
        val cell = cellsList.get(i)
        println("列簇 ： " + Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()) + " value : " +   Bytes.toString(cell.getValueArray, cell.getValueOffset, cell.getValueLength).toString())
      }

//      if (result != null) {
//
//        val cellsYl: java.util.List[Cell] = result.getColumnCells(Bytes.toBytes("info"), Bytes.toBytes("yearid"))
//        if (cellsYl != null) {
//
//          for (i <- 0 until cellsYl.size()) {
//            val cell = cellsYl.get(i)
//            var value = Bytes.toInt(cell.getValue)
//            var version = cell.getTimestamp
//
//            println("version : " + version + " yearids : " + value)
//          }
//        }
//
//        val cellsPN: java.util.List[Cell] = result.getColumnCells(Bytes.toBytes("info"), Bytes.toBytes("platenumber"))
//        if (cellsPN != null) {
//
//          for (i <- 0 until cellsPN.size()) {
//            val cell = cellsPN.get(i)
//            var value = Bytes.toString(cell.getValue)
//            var version = cell.getTimestamp
//
//            println("version : " + version + " platenumber : " + value)
//          }
//        }
//
//      }

      table.close()
    } catch {
      case ex: Exception => {
        println("IO Exception")
      }
    }
  }
}