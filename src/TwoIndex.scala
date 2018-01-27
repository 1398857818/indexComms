import java.util

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by sq on 2017/11/19.
  * 此文件无用的注释少
  * 比 TwoIndex2 更标准，查找 queryList 就可知了
  */
object TwoIndex {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("TrieIndex").setMaster("spark://hadoop01:7077") //"spark://hadoop01:7077"

    val sc = new SparkContext(conf)
    //val lines = sc.textFile("hdfs://hadoop01:9000/sq/jd_data/date2/*.txt")
    val lines = sc.textFile("hdfs://hadoop01:9000/sq/jd_data2/*/*.txt")
    //val lines =sc.textFile("D:/datas/jd_data/count2/100/*.txt")

    //val lines =sc.textFile("comms/*.txt")

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

    //建树
    val pttRDD = guRDD.mapPartitions(iter => {
      val pUid = new PTrieTree
      val pGU = new PTrieTree
      iter.foreach { tuple =>
        val key = tuple._1
        val comms = tuple._2
        comms.foreach(pGU.insert(key, _))

        val ukey = tuple._1.substring(8)
        val uvalue = tuple._1
        //        println("ukey" + ukey )
        //        if(ukey.equals("j***u")){
        //          println("uvalue" + uvalue)
        //        }
        pUid.insert(ukey, uvalue)
      }
      Iterator(pUid, pGU)
    }).cache()
    pttRDD.cache()
    pttRDD.count()
    pttRDD.count()



    //记录查询时间
    //记录查询时间
    val start = System.currentTimeMillis()
    val nanoStart = System.nanoTime()

    //"d***n,j***u"
    //"10019917,10014302"

    //val querySQL = "0696888"
    val queryGUSQL = "10019917j***u" //10019917j***u
    val queryUSet = "d***n,j***u" //j***w,p***8
    val qUSet = queryUSet.split(",")

    val queryGSet = "10019917,10014302"//10019917,10014302
    val qGSet = queryGSet.split(",")
    val queryRDD = pttRDD.mapPartitions { iter =>

      var Userptt = new PTrieTree
      var GUptt = new PTrieTree
      var count = 1
      while (iter.hasNext) {
        if (count == 1)
          Userptt = iter.next()
        if (count == 2)
          GUptt = iter.next()
        count += 1
      }
      import scala.collection.JavaConversions._
      import java.text.SimpleDateFormat

      val qList = new util.ArrayList[String]() //二级索引
      if (queryGUSQL.length >= 8) {
        if (qGSet.size >= 1 && qGSet(0)!= "" ) {
          println(" qGSet(0) " + qGSet(0))

          //println(" qGSet.size " + qGSet.size)
          //println(" qGSet.length " + qGSet.length)
          for (qString <- qGSet) {
            //println("qGSet" + qString)
            qList.addAll(GUptt.getIndex(qString))
          }
        }else {
          //println("GUptt.getIndex(queryGUSQL)")
          qList.addAll(GUptt.getIndex(queryGUSQL))
        }
        //        if(qSet.length > 1) {
        //          for(qString <- qSet){
        //            qList.addAll(GUptt.getIndex(qString))
        //          }
        //        }


      } else {
        val qGUSet = new util.HashSet[String]()
        //多组用户
        for (qString <- qUSet) {
          qGUSet.addAll(Userptt.getIndex(qString))
        }
        //一个用户名 查询
        //val qGUList = Userptt.getIndex("j***u") //返回Gid+ Uid(一级索引)  再次使用树去查找
        //println("j***u" + qGUList)
        //qGUSet.addAll(qGUList)

        //val qList = new util.ArrayList[String]() //二级索引
        for (key <- qGUSet) {
          //println("key   " + key)
          //println("GUptt.getIndex(key)  " +  GUptt.getIndex(key))
          qList.addAll(GUptt.getIndex(key)) //将返回list 累加
        }
      }




      def getLong(ymdhms: String): Long = {
        val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        val date = simpleDateFormat.parse(ymdhms)
        val ts = date.getTime()
        val res = String.valueOf(ts)
       // System.out.println(res)
        return ts
      }
      val startDate = getLong("2008-05-29 02:48:22")
      val endDate = getLong("2018-06-04 21:26:18")




      val filterQL = new util.ArrayList[String]()
      for (timeCom <- qList) {
        var tc = timeCom.split(",")
        if (tc.length == 2) {
          //timeCom 可能为err2，即qList查不到的结果
          var date = getLong(tc(0))

          if (date >= startDate && date <= endDate)
            filterQL.add(timeCom)
          // println("timeCom   " + timeCom)

        }
      }
      //println("filterQL.size()" + filterQL.size())
      Iterator(filterQL)

    }



    val ra = queryRDD.collect()
    val end = System.currentTimeMillis()
    val nanoEnd = System.nanoTime()

    ra.filter(_.size() != 0).foreach(x => println(x + "   ......................"))
    guRDD.unpersist()
    guRDD.unpersist()
    pttRDD.unpersist()
    pttRDD.unpersist()
    println("query time ------> " + (end - start) + "    " + (nanoEnd - nanoStart) / 1000000)
  }
}
