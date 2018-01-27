package com.sq.bptree

import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration, HTableDescriptor}
import org.apache.hadoop.hbase.client.{HBaseAdmin, Result}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by sq on 2017/12/27.
  * 无法获取时间
  */
class hbaseTwoBPlus {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("TrieIndex").setMaster("spark://hadoop01:7077")

    val sc = new SparkContext(conf)
    val tablename = "mycomm1"
    val hbaseConf = HBaseConfiguration.create()
    //在hbase-site.xml文件中可见
    hbaseConf.set("hbase.zookeeper.quorum","hadoop04,hadoop05,hadoop06,hadoop07,hadoop08")
    hbaseConf.set("hbase.zookeeper.property.clientPort","2181")
    hbaseConf.set("hbase.master","hadoop01:60010")
    hbaseConf.set(TableInputFormat.INPUT_TABLE,tablename)

    //如果表不存在则创建表
    val admin = new HBaseAdmin(hbaseConf)
    if(! admin.isTableAvailable(tablename)){
      val tableDesc = new HTableDescriptor(tablename)
      admin.createTable(tableDesc)
    }
    //读取数据并转换为RDD
    val hbaseRDD = sc.newAPIHadoopRDD(hbaseConf,classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[Result]).repartition(12)

    import scala.collection.JavaConversions._
    val pttRDD = hbaseRDD.mapPartitions { iter =>

      val pGU = new BPlusTree[String,String]
      iter.foreach { case(_,result) => {
        val key = Bytes.toString(result.getRow)
        val user = Bytes.toString(result.getValue("user".getBytes,"u1".getBytes))
        val comms = result.getColumnCells("comm".getBytes,user.getBytes)
        for(comcell <- comms){
          val GUrow = new String(CellUtil.cloneRow(comcell))
          val value = new String(CellUtil.cloneValue(comcell))

          pGU.set(GUrow,value)
        }
      }

      }
      Iterator(pGU)
    }.cache()
  }
}
