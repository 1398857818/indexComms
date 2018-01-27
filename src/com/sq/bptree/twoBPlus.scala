package com.sq.bptree

import java.util

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by sq on 2017/12/24.
  */
object twoBPlus {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("twoBPlus").setMaster("local")
    val sc = new SparkContext(conf)

    //val lines = sc.textFile("hdfs://hadoop01:9000/sq/jd_data/date2/*.txt")
    //val lines = sc.textFile("comms/*.txt")
    //val lines = sc.textFile("hdfs://hadoop01:9000/sq/jd_data2/100/*.txt")
    val lines =sc.textFile("D:/datas/jd_data/count2/100/*.txt")

    val splitedRDD = lines.map(line => {
      val splitedStr = line.split("\t")
      splitedStr
    })

    val guRDD = splitedRDD.filter(_.length == 4)
      .map(splitedStr => {
        val key = splitedStr(1) + "," + splitedStr(0)
        val value = splitedStr(2) + "-" + splitedStr(3)
        (key, value)
      }).groupByKey(12).cache()

    val bptRDD = guRDD.mapPartitions(iter => {
      val bptUser = new BPlusTree[String, String]
      val bptGUid = new BPlusTree[String, String]
      var arrayList = new util.ArrayList[String]()
      var comp = ""
      var flag = false

      iter.foreach { tuple =>
        val key = tuple._1
        val comms = tuple._2
        var keystr = tuple._1.split(",")
        var uid = keystr(0)
        if (flag == false || !comp.equals(uid)) {
          //以用户的数量少信息多的特征，插入范围查询起始的key 不合适吧，因为用户的数据量比较少
          bptUser.set(uid, "beg")
          if(uid.equals("j***w"))
            println("uid      " + uid)
          comp = uid
          flag = true
        }
        // comms.foreach(bpt.set(key, _))
        val value = keystr(1) + keystr(0)
        if (keystr(0).equals("j***w")) {
          println("Insert     " + value)
      }
        bptUser.set(key, value)

        //构建Gid 和 Uid  树
       // println("gid   "+ keystr(1) +"--------"+ keystr(0))
        if (keystr(1).length == 8) {
          var gid = (keystr(1) + keystr(0)).substring(0, 8)
          if (flag == false || !comp.equals(gid)) {
            bptGUid.set(gid, "beg")
            //println("gid      " + gid)
            comp = gid
            flag = true
          }
          comms.foreach(bptGUid.set((keystr(1) + keystr(0)), _))
        }
      }

      println("j***w,10696888V   " + bptUser.get("j***w,10696888"))
      Iterator(bptUser,bptGUid)
    }).cache()

    println(bptRDD.count())


    //只要能查出来
    //查询树
    val queryRDD = bptRDD.mapPartitions { iter => {

      //是不是这段代码没有执行，就开始计时了
      //因为直到触发collect，才开始执行这个代码的
      // 是执行了，只是太快了，不可能不到1ms，每个分区有425M到440M这么多，12个分区
      val start = System.currentTimeMillis()
      val nanoStart = System.nanoTime()


      var bptUser = new BPlusTree[String, String]
      var bptGUid = new BPlusTree[String, String]
      var count = 1
      while (iter.hasNext) {
        if (count == 1)
          bptUser = iter.next()
        if (count == 2)
          bptGUid = iter.next()
        count += 1
      }

      /**
        * 查询 Gid
        * 查询 Gid + Uid
        * 查询 Uid
        * 查询 Gid + Uid + Time
        */
      //返回Gid+ Uid(一级索引)  再次使用树去查找
      val querySQL = "10019917j***u"
      //val queryUserSet = "b***f"
      val queryUserSet = "d***n,j***u"
      val queryGidSet = "10019917,10014302"
      var gus = ""
      val qUserSet = queryUserSet.split(",")
      val qGidSet = queryGidSet.split(",")
      val qList = new util.ArrayList[String]() //二级索引
      var qStr = ""
      //if(){}
      if (querySQL.length > 8) {

          qList.add(bptGUid.get(querySQL))
          println("querySQL" + bptGUid.get(querySQL))
          qStr = qStr + bptGUid.get(querySQL)

      } else if (!queryUserSet.equals("")){
        // val qGUSet = new util.HashSet[String]()
        val qGUSet = new util.ArrayList[String]()
        //多组用户

        for (qString <- qUserSet) {
          //qGUSet.add(bptUser.get(qString))
          //bptUser.get(qString)  //这个可以查出什么 beg.beg...
          bptUser.getall()
          println("j***w,10696888   " + bptUser.dataKeys.indexOf("j***w,10696888"))
          println("j***w,10696888V   " + bptUser.get("j***w,10696888"))
          val qdKU = bptUser.dataKeys.indexOf(qString)
          if (qdKU != -1) {
            var con = true
            var i = qdKU + 1
            while (con) {
              if (!bptUser.dataKeys.get(i).split(",")(0).equals(qString)) {
                con = false
              }
              if (con == true) {
                //获取 j***w 的全部商品
                val value = bptUser.dataValues.get(i)
                println("j***wK    " + bptUser.dataKeys.get(i) + "   " + value)
                i += 1
                // j***w 的所有Gid+Uid
                gus = gus + "," + value
                println("gus  " + gus)

              }
            }
            //        val qdV = bptUser.dataValues.get(qdK)
            //        println("qdV    " + qdV)
          }
          var gulist = gus.split(",")
          println("gulist.size  " + gulist.size + "  " + gus)
          if (gulist.size > 1) {
            for (j <- 1 to (gulist.size - 1)) {
              println("gulist.size > 1" + "  j  " + j)
              var gu = gulist(j)
              bptGUid.get(gu)
              qList.add(bptGUid.get(gu))
            }
          }
          //     qStr = qStr + bptUser.get(String)
          //  多次执行一个用户名查询吗

        }


      }else {
        for (qString <- qGidSet) {

          //查询一个商品10696888
          bptGUid.getall()
          val qdKg = bptGUid.dataKeys.indexOf(qString)
          //存放某一商品的全部评论信息
          var arrayList = new util.ArrayList[String]()
          println("qdK    " + qdKg)

          //查找 10696888商品的全部评论
          if (qdKg != -1) {
            var con = true
            var i = qdKg + 1
            while (con) {
              if(i < bptGUid.dataKeys.size()) {
                println(i + "   " + bptGUid.dataKeys.size() + con)

//                if (bptGUid.dataKeys.get(i).length == 8) {

                  if (!bptGUid.dataKeys.get(i).substring(0, 8).equals(qString)) {
                    con = false
                  }
                  if (con == true) {
                    val value = bptGUid.dataValues.get(i)
                    println(bptGUid.dataKeys.get(i))
                    //println("1069V    " + bptGUid.dataKeys.get(i) + value)
                    i += 1
                    if( i == 10813){
                      println(bptGUid.dataKeys.get(i))
                      con = false
                    }
                    qList.add(value) //评论数据
                  }
 //               }
              }
              if(i ==  bptGUid.dataKeys.size()){
                con = false
              }
            }

            val qdV = bptGUid.dataValues.get(qdKg)
            println("qdV    " + qdV)
          }
        }
      }



        //根据上一棵树，获取的结果，逐次查询想要获得的结果

//        val qString = bptGUid.get("10696888j***w")
//        println(qString)
//        var results = new util.ArrayList[String]()
//        //var result = ""
//        val guArray = gus.split(",")
//        for (str <- guArray) {
//          if (!str.equals("")) {
//            val qStr = bptGUid.get(str)
//            results.add(qStr)
//            //results = results + "," + qStr
//          }
//        }

      import java.text.SimpleDateFormat
      def getLong(ymdhms: String): Long = {
        val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        val date = simpleDateFormat.parse(ymdhms)
        val ts = date.getTime()
        val res = String.valueOf(ts)
        System.out.println(res)
        return ts
      }
      val startDate = getLong("2011-05-29 02:48:22")
      val endDate = getLong("2017-06-04 21:26:18")

      import scala.collection.JavaConversions._
      import scala.collection.JavaConversions._


      val filterQL = new util.ArrayList[String]()
      if (qList != null) {
      for (qrStr <- qList) {

      if (qrStr != null) {
        val qcomms = qrStr.split(";")
        for (timeCom <- qcomms) {
          var tc = timeCom.split("-")
          if (tc.length == 2) {
            if(tc(0).matches("\\d{4}-\\d{1,2}-\\d{1,2}\\s\\d{2}:\\d{2}:\\d{2}")) {
              println(tc(0))
              var date = getLong(tc(0))

              if (date >= startDate && date <= endDate)
                filterQL.add(timeCom)
            }
          }
        }

        val end = System.currentTimeMillis()
        val nanoEnd = System.nanoTime()
        // println("query time ------> " + nanoEnd + "  " + nanoStart + (end - start) + "    " + (nanoEnd - nanoStart))
        // Iterator(qList)
        // Iterator(filterQL)

      }
    }
    }
      // 注意返回类型
      //if(){

      //}
      Iterator(qList)
      //Iterator(arrayList)
    }}

    val ra = queryRDD.collect()
    ra.filter(_.size() != 0).foreach(x => println(x + "   ......................"))



  }
}
