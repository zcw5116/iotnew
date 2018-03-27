package com.zyuc.stat.iot.user

import java.text.SimpleDateFormat
import java.util.Calendar

import com.zyuc.stat.utils.HbaseUtils
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable
import scala.collection.mutable.Set
import scala.io.Source

/**
 * Created by wangpf on 2017/6/30.
 * desc:计算出统计时间到统计时间后12小时的企业在线用户数的基线
 */
object IotOnlineUsernumBaseline {
  private val hBaseConf = HbaseUtils.getHbaseConf("EPC-LOG-NM-15,EPC-LOG-NM-17,EPC-LOG-NM-16", "2181")

  def main(args: Array[String]) {
    val fileName = "/home/slview/test/wangpf/BPFile/IotOnlineUsernumBaseline.BP"
    val (baselineTime, beginTime, endTime, dayArray) = getStatistime(fileName)

    // 创建上下文
    val sparkConf = new SparkConf()
      .setAppName("IotOnlineUsernumBaseline")

    val sc = new SparkContext(sparkConf)

    // 创建hiveContext
    val hiveContext = new HiveContext(sc)
    // 进行隐式转换
    import hiveContext.implicits._

    var rdd: RDD[(String, String, Long, Long)] = null

    dayArray.foreach { day =>
      if (rdd == null)
        rdd = scanRecordToRdd(sc, "iot_online_users_" + day)
      else
        rdd.union(scanRecordToRdd(sc, "iot_online_users_" + day))
    }

    rdd
      .toDF("companycode", "Time", "c_3g_cnt", "c_4g_cnt")
      .registerTempTable("IotOnlineUsernumBaselineTempTable")

    val sql =
      "select companycode, Time, avg(c_3g_cnt), avg(c_4g_cnt) " +
        "from IotOnlineUsernumBaselineTempTable " +
        s"where Time >= ${beginTime} " +
        s"and Time < ${endTime} " +
        "group by companycode, Time"

    val jobconf = HbaseUtils.getHbasejobConf(hBaseConf, "iot_online_users_" + baselineTime)
    // 批量插入hbase
    hiveContext.sql(sql).coalesce(1).rdd.map( x => {
      val put = new Put(Bytes.toBytes(x(0) + "_" + x(1)))
      //为put操作指定 column 和 value
      put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("b_3g_cnt"), Bytes.toBytes(x(2).toString))
      put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("b_4g_cnt"), Bytes.toBytes(x(3).toString))

      (new ImmutableBytesWritable, put)
    })
      .saveAsHadoopDataset(jobconf)

    sc.stop
  }

  /**
   * Created by wangpf on 2017/6/30.
   * desc:根据参数获取时间，基线取前7天数据进行计算
   */
  def getStatistime(fileName: String): (String, String, String, mutable.Set[String]) ={
    var baselineTime: String = null
    var beginTime: String = null
    var endTime: String = null
    val dayArray: Set[String] = Set()
    // 从文件中获取参数
    val source = Source.fromFile(fileName)
    val time = source.getLines.next()
    source.close

    val m = "[0-9]{12}".r
    time match {
      case m() => {
        val sdf = new SimpleDateFormat("yyyyMMddHH")
        // 获取基准值的时间
        val inDate = sdf.parse(time)
        val calendar = Calendar.getInstance()
        // 获取当前统计时间
        beginTime = time.substring(8, 10) + "00"

        // 文件的时间加上12小时为处理的结束时间
        calendar.setTime(inDate)
        calendar.add(Calendar.HOUR, 12)
        endTime = sdf.format(calendar.getTime()).substring(8, 10) + "00"

        for (x <- 1 to 7) {
          calendar.setTime(inDate)
          calendar.add(Calendar.HOUR, -24*x)
          dayArray.add(sdf.format(calendar.getTime()).substring(0, 8))
        }

        calendar.setTime(inDate)
        calendar.add(Calendar.HOUR, 24)
        baselineTime = sdf.format(calendar.getTime()).substring(0, 8)
      }
      case other => {
        println("param error!")
        System.exit(0)
      }
    }

    println("baselineTime: " + baselineTime)
    println("beginTime: " + beginTime)
    println("endTime: " + endTime)
    println("dayArray: " + dayArray)
    (baselineTime, beginTime, endTime, dayArray)
  }

  /**
   * Created by wangpf on 2017/6/30.
   * desc:扫描记录 并转成DataFrame，过滤掉不存在字段的数据
   */
  def scanRecordToRdd(sc: SparkContext, tablename: String): RDD[(String, String, Long, Long)] = {
    // 创建hbase configuration
    hBaseConf.set(TableInputFormat.INPUT_TABLE, tablename)
    hBaseConf.set(TableInputFormat.SCAN_COLUMN_FAMILY, "info")

    // 读取数据并转化成rdd
    val hBaseRDD = sc.newAPIHadoopRDD(hBaseConf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    // 将RDD转化为dataframe schema
    val shop = hBaseRDD.map( r => (
      Bytes.toString(r._2.getRow).split("_")(0),
      Bytes.toString(r._2.getRow).split("_")(1),
      if (r._2.containsColumn(Bytes.toBytes("info"), Bytes.toBytes("c_3g_cnt")))
        Bytes.toString(r._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("c_3g_cnt"))).toLong
      else
        -9999999999L,
      if (r._2.containsColumn(Bytes.toBytes("info"), Bytes.toBytes("c_4g_cnt")))
        Bytes.toString(r._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("c_4g_cnt"))).toLong
      else
        -9999999999L
      ))
      .filter(_._3 != -9999999999L)
      .filter(_._4 != -9999999999L)

    shop
  }
}
