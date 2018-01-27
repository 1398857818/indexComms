


import java.util

import org.apache.hadoop.hbase.client.{HBaseAdmin, Result}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration, HTableDescriptor}
import org.apache.spark.{SparkConf, SparkContext}


/**
  * Created by sq on 2017/11/21.
  * 配合hbase
  */
object TwoIndex3 {
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

//    hbaseRDD.foreach{case(_,result) => {
//      //获取行健
//      val key = Bytes.toString(result.getRow)
//      val timeStamp = Bytes.toString(result.getRow)
//      //通过列族 和 列名获取列
//      val user = Bytes.toString(result.getValue("user".getBytes,"u1".getBytes))
//      val comms = result.getColumnCells("comm".getBytes,user.getBytes)
//      for(comcell <- comms){
//        val row = CellUtil.cloneRow(comcell)
//
//        // 截取字符串吗
//      }
//      println("Row key:" + key + " Name:" )
//    }}
    import scala.collection.JavaConversions._
    val pttRDD = hbaseRDD.mapPartitions { iter =>

      val pGU = new PTrieTree
      iter.foreach { case(_,result) => {
        val key = Bytes.toString(result.getRow)
        //获取用户名
        val user = Bytes.toString(result.getValue("user".getBytes,"u1".getBytes))
        //获取评论
        val comms = result.getColumnCells("comm".getBytes,user.getBytes)

        for(comcell <- comms){
          val GUrow = new String(CellUtil.cloneRow(comcell))
          val value = new String(CellUtil.cloneValue(comcell))

          pGU.insert(GUrow,value)
        }
      }
        }
      Iterator(pGU)
    }.cache()
    pttRDD .count()


    //val lines = sc.textFile("hdfs://hadoop01:9000/sq/jd_data/date2/*.txt")
    //val lines =sc.textFile("comms/*.txt")

//    val splitedRDD = lines.map(line => {
//      val splitedStr = line.split("\t")
//      splitedStr
//    })

    // val idList = splitedRDD.map( x => (x(0) + x(1)) ).collect()

//    val rp = System.currentTimeMillis()
//    val rpStart = System.nanoTime()
//
//    val guRDD = splitedRDD.filter(_.length == 4)
//      .map(splitedStr => {
//        val key = splitedStr(0) + splitedStr(1)
//        val value = splitedStr(2) + "," + splitedStr(3)
//        (key, value)
//      }).groupByKey(12).cache() //这里保存了shuffle从磁盘到读到内存（被分割后最后形成12分区的内存状态吗）的状态吗
//
//    //建树
//    val pttRDD = guRDD.mapPartitions(iter => {
//      val pUid = new PTrieTree
//      val pGU = new PTrieTree
//      iter.foreach { tuple =>
//        val key = tuple._1
//        val comms = tuple._2
//        comms.foreach(pGU.insert(key, _))
//
//        val ukey = tuple._1.substring(7)
//        val uvalue = tuple._1
//        pGU.insert(ukey, uvalue)
//      }
//      Iterator(pUid, pGU)
//    }).cache()
//    pttRDD.count()



    //记录查询时间

    val start = System.currentTimeMillis()
    val nanoStart = System.nanoTime()

    val querySQL = "10830829p***8"
    val querySet = "j***w,p***8"
    val qSet = querySet.split(",")
    val queryRDD = pttRDD.mapPartitions { iter =>


      var Userptt = new PTrieTree
      var GUptt = new PTrieTree
      var count = 1
      while (iter.hasNext) {
        if (count == 2)
          Userptt = iter.next()
        if (count == 1)
          GUptt = iter.next()
        count += 1
      }
      import java.text.SimpleDateFormat

      import scala.collection.JavaConversions._

      val qList = new util.ArrayList[String]() //二级索引
      if (querySQL.length >= 8) {
        qList.addAll(GUptt.getIndex(querySQL))
        //        if(qSet.length > 1) {
        //          for(qString <- qSet){
        //            qList.addAll(GUptt.getIndex(qString))
        //          }
        //        }


      } else {
        val qGUSet = new util.HashSet[String]()
        //多组用户
        for(qString <- qSet){
          qGUSet.addAll(Userptt.getIndex(qString))
        }
        //一个用户名 查询
        val qGUList = Userptt.getIndex("o***6") //返回Gid+ Uid(一级索引)  再次使用树去查找

        qGUSet.addAll(qGUList)

        //val qList = new util.ArrayList[String]() //二级索引
        for (key <- qGUSet) {
          qList.addAll(GUptt.getIndex(key)) //将返回list 累加
        }
      }


      /**
        *
        * 15.856361 s
        * @param ymdhms
        * @return
        */

      def getLong(ymdhms: String): Long = {
        val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        val date = simpleDateFormat.parse(ymdhms)
        val ts = date.getTime()
        val res = String.valueOf(ts)
        System.out.println(res)
        return ts
      }
      val startDate = getLong("2016-05-29 02:48:22")
      val endDate = getLong("2016-06-04 21:26:18")




      val filterQL = new util.ArrayList[String]()
      for (timeCom <- qList) {
        var tc = timeCom.split(",")
        if (tc.length == 1 && (timeCom.contains("err2")== false)) {
          //timeCom 可能为err2，即qList查不到的结果
//          println(tc(0))
//          var date = getLong(tc(0))
//
//          if (date >= startDate && date <= endDate)
            filterQL.add(timeCom)
        }
      }
      Iterator(filterQL)

    }
    val end = System.currentTimeMillis()
    val nanoEnd = System.nanoTime()

    println("query time ------> " + (end - start) + "    " + (nanoEnd - nanoStart) / 1000000)
    val ra = queryRDD.collect()
    ra.filter(_.size() != 0).foreach(x => println(x + "   ......................"))

  }
}
