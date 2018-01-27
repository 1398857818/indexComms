package com.sq.bptree

import java.util

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by sq on 2017/12/21.
  */
object TestBplus {

    def main(args: Array[String]) {
      val conf = new SparkConf().setAppName("TrieIndex").setMaster("spark://hadoop01:7077")

      val sc = new SparkContext(conf)
      //val lines = sc.textFile("hdfs://hadoop01:9000/sq/jd_data/date2/*.txt")
      //val lines = sc.textFile("hdfs://hadoop01:9000/sq/jd_data2/100/*.txt")
     // val lines =sc.textFile("comms/*.txt")
      val lines =sc.textFile("D:/datas/jd_data/count2/100/*.txt")
      //val lines =sc.textFile("D:/datas/jd_data/count2/[1-200]/*.txt")

      val splitedRDD = lines.map(line => {
        val splitedStr = line.split("\t")
        splitedStr
      })

      // val idList = splitedRDD.map( x => (x(0) + x(1)) ).collect()

      val rp = System.currentTimeMillis()
      val rpStart = System.nanoTime()

      val guRDD = splitedRDD.filter(_.length == 4)
        .map(splitedStr => {
          val key = splitedStr(0) + splitedStr(1)
          val value = splitedStr(2) + "," + splitedStr(3)
          (key, value)
        }).groupByKey(12).cache() //这里保存了shuffle从磁盘到读到内存（被分割后最后形成12分区的内存状态吗）的状态吗
      //.repartition(12).cache()
      //guRDD.first()

      //生成索引RDD IndexedRDD
      //val indexGU = IndexedRDD(guRDD)

      /**
        * indexed2.get(1234L) // => Some(10873)
        * 通过IndexedRDD，即可查询到，没必要通过树了
        * 但是key只能是 long型
        */
      //indexGU.get("10696888j***m")

      val cindex = System.currentTimeMillis()
      val cindexStart = System.nanoTime()

      import scala.collection.JavaConversions._

      //构建树，插入结点
      val bptRDD = guRDD.mapPartitions(iter => {
        val bpt = new BPlusTree[String,String]
        var comp = ""
        var flag = false
        var arrayList = new util.ArrayList[String]()
        iter.foreach { tuple =>
          val key = tuple._1
          val comms = tuple._2

          var gid = tuple._1.substring(0,8)
          if(flag == false  || !comp.equals(gid)){

            bpt.set(gid, "beg")
            println("gid      " + gid)
            comp = gid
            flag= true
          }
          comms.foreach(bpt.set(key, _))

        }
        val qString = bpt.get("10696888")
        bpt.getall()
        val qdK = bpt.dataKeys.indexOf("10696888")
        println("qdK    " + qdK)

        if(qdK != -1) {
          import scala.util.control._


          //        for(  i <- (qdK+1) to bpt.dataKeys.size()){
          //
          //          if(!bpt.dataKeys.get(i).substring(0,8).equals("10696888")){
          //            i = bpt.dataKeys.size()
          //          }
          //          val value = bpt.dataValues.get(i)
          //          println("1069V    " + bpt.dataKeys.get(i)+ value)
          //        }
          var con = true
          var i = qdK + 1

          while (con) {
            if (!bpt.dataKeys.get(i).substring(0, 8).equals("10696888")) {
              con = false
            }
            if (con == true) {
              // 获取10696888商品的全部评论
              val value = bpt.dataValues.get(i)
              println("1069V    " + bpt.dataKeys.get(i) + value)
              i += 1

              arrayList.add(value)
            }
          }

          val qdV = bpt.dataValues.get(qdK)
          println("qdV    " + qdV)
        }

        // println(qString)
        // println(bpt.height())
        Iterator(bpt)
        //Iterator(arrayList)
      }).cache()
      println(bptRDD.count())



      //记录查询时间
      val start = System.currentTimeMillis()
      val nanoStart = System.nanoTime()

      //查询树
      val queryRDD = bptRDD.mapPartitions { iter => {

        //是不是这段代码没有执行，就开始计时了
        //因为直到触发collect，才开始执行这个代码的
        // 是执行了，只是太快了，不可能不到1ms，每个分区有425M到440M这么多，12个分区
        val start = System.currentTimeMillis()
        val nanoStart = System.nanoTime()
        var arrayList = new util.ArrayList[String]()
        var bpt = new BPlusTree[String,String]
        while (iter.hasNext) {
          bpt = iter.next()

        }
      //  val qString = bpt.get("10696888")
        bpt.getall()
        val qdK = bpt.dataKeys.indexOf("10696888")
        println("qdK    " + qdK)

        if(qdK != -1) {

          var con = true
          var i = qdK + 1

          while (con) {
            if (!bpt.dataKeys.get(i).substring(0, 8).equals("10696888")) {
              con = false
            }
            if (con == true) {
              val value = bpt.dataValues.get(i)
              println("1069V    " + bpt.dataKeys.get(i) + value)
              i += 1

              arrayList.add(value)
            }
          }

          val qdV = bpt.dataValues.get(qdK)
          println("qdV    " + qdV)
        }




        val qString = bpt.get("10696888j***w")
        println(qString)

        import java.text.SimpleDateFormat
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

        import scala.collection.JavaConversions._
        import scala.collection.JavaConversions._

        val filterQL = new util.ArrayList[String]()
        if(qString != null) {
          val qList = qString.split(";")
          for (timeCom <- qList) {
            var tc = timeCom.split(",")
            if (tc.length == 2) {
              //timeCom 可能为err2，即qList查不到的结果
              println(tc(0))
              var date = getLong(tc(0))

              if (date >= startDate && date <= endDate)
                filterQL.add(timeCom)
            }
          }


          //      if(!qList.contains("err2")){
          //         Iterator(qList)
          //      }else{
          //        Iterator(Nil)
          //      } 是因为有两个返回值，所以报错吗

          //println(qList + "getIndex")
          val end = System.currentTimeMillis()
          val nanoEnd = System.nanoTime()
          //  println("query time ------> " + nanoEnd + "  " + nanoStart + (end - start) + "    " + (nanoEnd - nanoStart))
          //Iterator(qList)
          //Iterator(filterQL)
        }
        Iterator(qString)
      }}


      val ra = queryRDD.collect()

      val end = System.currentTimeMillis()
      val nanoEnd = System.nanoTime()

      println("query time ------> " + (end - start) + "    " + (nanoEnd - nanoStart) / 1000000)
      println("create index time ------> " + (start - cindex) + "   " + (nanoStart - cindexStart) / 1000000)
      println("repartition time ------> " + (cindex - rp) / 1000 + "   " + (cindexStart - rpStart) / 1000000000)
      //一个分区就是一个task吗，那么mapPartitions的执行时间，就是最大的task的执行时间吗

      //ra.filter((!_.contains("err2"))  ).foreach(x => println(x + "   ......................"))
      ra.filter(_ != null).foreach(x => println(x + "   ......................"))
      //    val tt = new TrieTree()
      //    val indexs = tt.getIndex("10696888j***m")




    }


}
