import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration, HTableDescriptor}
import org.apache.hadoop.hbase.client.{HBaseAdmin, Result}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by sq on 2017/11/30.
  */
class NonIndexHBa {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("TrieIndex").setMaster("spark://hadoop01:7077")

    val sc = new SparkContext(conf)
    val tablename = "mycomm3_7"
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
    hbaseRDD.foreach{case(_,result) => {
      //获取行健
      val key = Bytes.toString(result.getRow)
      val timeStamp = Bytes.toString(result.getRow)
      //通过列族 和 列名获取列
      val user = Bytes.toString(result.getValue("user".getBytes,"u1".getBytes))
      val comms = result.getColumnCells("comm".getBytes,user.getBytes)
      for(comcell <- comms){
        val row = CellUtil.cloneRow(comcell)

        // 截取字符串吗
      }
      println("Row key:" + key + " Name:" )
    }}



  }

}
