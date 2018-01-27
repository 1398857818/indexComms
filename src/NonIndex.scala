import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by sq on 2017/11/2.
  */
object NonIndex {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("TrieIndex").setMaster("spark://hadoop01:7077")

    val sc = new SparkContext(conf)
    val lines = sc.textFile("hdfs://hadoop01:9000/sq/jd_data/date2/*.txt")

    //val lines =sc.textFile("comms/*.txt")

    val splitedRDD = lines.map(line => {
      val splitedStr = line.split("\t")
      splitedStr
    })

    // val idList = splitedRDD.map( x => (x(0) + x(1)) ).collect()


    val guRDD = splitedRDD.filter(_.length == 4)
      .map(splitedStr => {
        val key = splitedStr(0) + splitedStr(1)
        val value = splitedStr(2) + "," + splitedStr(3)
        (key, value)
      }).groupByKey(12).cache()

    guRDD.first()


    val start = System.currentTimeMillis()
    val nanoStart = System.nanoTime()

    val res = guRDD.filter(_._1.equals("10696888j***m") ).collect()
    val end = System.currentTimeMillis()
    val nanoEnd= System.nanoTime()

    println("query time ------> "+ (end - start )+ "    " + (nanoEnd - nanoStart)/1000000)
  }
}
