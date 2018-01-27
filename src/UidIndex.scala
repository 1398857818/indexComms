import java.util

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by sq on 2017/11/18.
  */
object UidIndex {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("TrieIndex").setMaster("spark://hadoop01:7077")

    val sc = new SparkContext(conf)
    val lines = sc.textFile("hdfs://hadoop01:9000/sq/jd_data/date2/*.txt")

    //val lines =sc.textFile("comms/*.txt")

    val splitedRDD = lines.map(line => {
      val splitedStr = line.split("\t")
      splitedStr
    })
    val querySQL = "j***w10696888"
    //如果是商品ID ,则以商品ID为key
  val guRDD =
      if(querySQL.equals("j***w10696888")) {
        splitedRDD.filter(_.length == 4)
          .map(splitedStr => {
            val key = splitedStr(1) + splitedStr(0)
            val value = splitedStr(2) + "," + splitedStr(3)
            (key, value)
          }).groupByKey(12).cache()
      }else{ //如果是用户ID，则以用户ID为key
        splitedRDD.filter(_.length == 4)
          .map(splitedStr => {
            val key = splitedStr(0) + splitedStr(1)
            val value = splitedStr(2) + "," + splitedStr(3)
            (key, value)
          }).groupByKey(12).cache()
      }

    //建树
    val pttRDD = guRDD.mapPartitions(iter => {
      val ptt = new PTrieTree
      iter.foreach { tuple =>
        val key = tuple._1
        val comms = tuple._2
        comms.foreach(ptt.insert(key, _))

      }
      Iterator(ptt)
    }).cache()
    pttRDD.count()


    //记录查询时间
    val start = System.currentTimeMillis()
    val nanoStart = System.nanoTime()

    val queryRDD = pttRDD.mapPartitions { iter =>


      var ptt = new PTrieTree
      while (iter.hasNext) {
        ptt = iter.next()

      }
      val qList = ptt.getIndex("j***w10696888")

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


      val filterQL = new util.ArrayList[String]()
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
      Iterator(filterQL)

    }
    val end = System.currentTimeMillis()
    val nanoEnd = System.nanoTime()

    println("query time ------> " + (end - start) + "    " + (nanoEnd - nanoStart) / 1000000)
    val ra = queryRDD.collect()
    ra.filter(_.size() != 0).foreach(x => println(x + "   ......................"))


  }
}

