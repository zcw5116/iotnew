package com.zyuc.stat.iot.user

import java.io.InputStream
import java.text.SimpleDateFormat
import java.util.Calendar

import com.zyuc.stat.tools.GetProperties
import com.zyuc.stat.utils.HbaseUtils
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{Logging, SparkConf, SparkContext}

import scala.collection.mutable.Map

/**
 * Created by wangpf on 2017/6/26.
 * desc:计算出统计时间到统计时间后五分钟之间的上下线用户数
 */

object IotUserLineDetail extends GetProperties with Logging {
  override def inputStreamArray: Array[InputStream] = Array(
    this.getClass.getClassLoader.getResourceAsStream("hbase.proerties")
  )
  // 获取配置文件的内容
  private val prop = props
  // 获取hbase配置
  private val hBaseConf = HbaseUtils.getHbaseConf(
    prop.getProperty("hbase.zookeeper.quorum"),
    prop.getProperty("hbase.zookeeper.property.clientPort"))

  def main(args: Array[String]) {
    // 获取统计日期
    val statisArray = getStatisTime(args)
    // 创建上下文
    val sparkConf = new SparkConf()
      .setAppName("IotUserLineDetail")

    val sc = new SparkContext(sparkConf)

    // 创建hiveContext
    val hiveContext = new HiveContext(sc)

    for (i <- 0 until statisArray.length) {
      val paramsMap = statisArray(i)
      logInfo("Deal params : " + paramsMap)
      // 处理此周期的数据
      dealData(sc, hiveContext, paramsMap)
    }

    sc.stop()
  }

  private def dealData(sc: SparkContext,
                       sqlContext: SQLContext,
                       paramsMap: Map[String, String]) {
    // 获取分析表数据并注册成表
    import sqlContext.implicits._
    scanAnalyToRdd(sc)
      .toDF(
      "company",
      "totalLogout_p_3g_cnt",
      "totalLogout_p_4g_cnt",
      "normalLogout_p_3g_cnt",
      "normalLogout_p_4g_cnt")
      .registerTempTable("AnalyregisterTempTable")

    val sql =
      "select iubi.vpdncompanycode, po.TerminateCause, " +
        " count(case when po.NetType = '3G' and po.Status = 'Start' then 1 else null end), " +
        " count(case when po.NetType = '3G' and po.Status = 'Stop' then 1 else null end), " +
        " count(case when po.NetType = '4G' and po.Status = 'Start' then 1 else null end), " +
        " count(case when po.NetType = '4G' and po.Status = 'Stop' then 1 else null end), " +
        " count(case when po.NetType = '3G' and po.Status = 'Stop' and po.TerminateCause in ('UserRequest', '') then 1 else null end), " +
        " count(case when po.NetType = '4G' and po.Status = 'Stop' and po.TerminateCause in ('UserRequest', '') then 1 else null end), " +
        " max(if totalLogout_p_3g_cnt is null, 0, totalLogout_p_3g_cnt), " +
        " max(if totalLogout_p_3g_cnt is null, 0, totalLogout_p_4g_cnt), " +
        " max(if totalLogout_p_3g_cnt is null, 0, normalLogout_p_3g_cnt), " +
        " max(if totalLogout_p_3g_cnt is null, 0, normalLogout_p_4g_cnt) " +
        "from iot.iot_user_basic_info iubi " +
        "inner join iot.pgwradius_out po " +
        "on iubi.mdn = po.mdn " +
        s" and po.dayid = '${paramsMap("statisday")}' " +
        s" and time >= '${paramsMap("startTime")}' " +
        s" and time < '${paramsMap("endTime")}' " +
        "left outer join AnalyregisterTempTable ar " +
        "on iubi.vpdncompanycode = ar.company " +
        "group by iubi.vpdncompanycode, po.TerminateCause with rollup "

    logInfo("sql = " + sql)

    val resault = sqlContext.sql(sql)

    val detailOnlineUsersJobconf = HbaseUtils.getHbasejobConf(
      hBaseConf,
      prop.getProperty("iot_detail_online_users_prefix") + paramsMap("statisDay"))
    val analyzeRstTabJobconf = HbaseUtils.getHbasejobConf(
      hBaseConf,
      prop.getProperty("analyze_rst_tab"))

    val data =
    resault
      .coalesce(3)
      .rdd
      .filter(_(0) != null)
      .map(row => {
      val value_3gStart = row(2).toString
      val value_3gStop = row(3).toString
      val value_4gStart = row(4).toString
      val value_4gStop = row(5).toString

      val company = if (row(0).toString.length == 0) "N999999999" else row(0).toString
      // 入iot_detail_online_users上下线明细表数据
      val userPut = new Put((company + "_" + paramsMap("statishhmm")).getBytes)
      // 入analyze_rst_tab分析表数据
      var analyzePut: Put = null

      if (row(1) == null) {
        val value_normal_3gStop = row(6).toString
        val value_normal_4gStop = row(7).toString
        val totalLogout_p_3g_cnt = row(8).toString
        val totalLogout_p_4g_cnt = row(9).toString
        val normalLogout_p_3g_cnt = row(10).toString
        val normalLogout_p_4g_cnt = row(11).toString

        userPut.addColumn("lineinfo".getBytes, "3gStart".getBytes, value_3gStart.getBytes())
        userPut.addColumn("lineinfo".getBytes, "3gStop".getBytes, value_3gStop.getBytes())
        userPut.addColumn("lineinfo".getBytes, "4gStart".getBytes, value_4gStart.getBytes())
        userPut.addColumn("lineinfo".getBytes, "4gStop".getBytes, value_4gStop.getBytes())
        // 实时计算写入分析表
        if (paramsMap("flag") == "1") {
          analyzePut = new Put(company.toString.getBytes)
          analyzePut.addColumn("alarmChk".getBytes, "logout_c_time".getBytes, (paramsMap("statisday") + paramsMap("statishhmm")).getBytes())
          analyzePut.addColumn("alarmChk".getBytes, "totalLogout_c_3g_cnt".getBytes, value_3gStop.getBytes())
          analyzePut.addColumn("alarmChk".getBytes, "totalLogout_c_4g_cnt".getBytes, value_4gStop.getBytes())
          analyzePut.addColumn("alarmChk".getBytes, "normalLogout_c_3g_cnt".getBytes, value_normal_3gStop.getBytes())
          analyzePut.addColumn("alarmChk".getBytes, "normalLogout_c_4g_cnt".getBytes, value_normal_4gStop.getBytes())

          analyzePut.addColumn("alarmChk".getBytes, "totalLogout_p_3g_cnt".getBytes, totalLogout_p_3g_cnt.getBytes())
          analyzePut.addColumn("alarmChk".getBytes, "totalLogout_p_4g_cnt".getBytes, totalLogout_p_4g_cnt.getBytes())
          analyzePut.addColumn("alarmChk".getBytes, "normalLogout_p_3g_cnt".getBytes, normalLogout_p_3g_cnt.getBytes())
          analyzePut.addColumn("alarmChk".getBytes, "normalLogout_p_4g_cnt".getBytes, normalLogout_p_4g_cnt.getBytes())
        }
      } else {
        val TerminateCause = if (row(1).toString == "") "null" else row(1).toString
        userPut.addColumn("termcase".getBytes, (TerminateCause + "_3g_cnt").getBytes, value_3gStop.getBytes())
        userPut.addColumn("termcase".getBytes, (TerminateCause + "_4g_cnt").getBytes, value_4gStop.getBytes())
      }

      ((new ImmutableBytesWritable, userPut), (new ImmutableBytesWritable, analyzePut))
    })

    data.persist()
    data.map(_._1).saveAsHadoopDataset(detailOnlineUsersJobconf)
    data.map(_._2).saveAsHadoopDataset(analyzeRstTabJobconf)
    data.unpersist()
  }

  /**
   * Created by wangpf on 2017/6/28.
   * desc:根据参数获取处理时间
   */
  private def getStatisTime(args: Array[String]): Array[Map[String, String]] ={
    val statisArray: Array[Map[String, String]] = null
    if (args.length == 4) {
      var startTime: String = args(0)
      val endTime: String = args(1)
      val period: Int = args(2).toInt
      val flag: String = args(3)

      val m = "[0-9]{12}".r
      startTime match {
        case m() => {
          endTime match {
            case m() => {
              // 遍历开始结束时间，获取统计日期数组
              val sdf = new SimpleDateFormat("yyyyMMddHHmmss")
              val calendar = Calendar.getInstance()
              // 计数
              var count = 0
              while (startTime < endTime) {
                val statisMap = Map[String, String]()
                val inDate = sdf.parse(startTime)
                // 统计标志 1代表实时计算 0代表离线计算
                statisMap("flag") = flag
                // 统计开始时间
                statisMap("startTime") = startTime
                statisMap("statisday") = startTime.substring(0, 8)
                statisMap("statishhmm") = startTime.substring(8, 12)

                // 获取上个周期的日期
                calendar.setTime(inDate)
                calendar.add(Calendar.MINUTE, -1*period)
                val lastPeriodTime = sdf.format(calendar.getTime())
                statisMap("lastPeriodday") = lastPeriodTime.substring(0, 8)
                statisMap("lastPeriodhhmm") = lastPeriodTime.substring(8, 12)

                // 获取下个循环的日期
                calendar.setTime(inDate)
                calendar.add(Calendar.MINUTE, period)
                startTime = sdf.format(calendar.getTime())
                // 统计结束时间
                statisMap("endTime") = startTime

                statisArray(count) = statisMap
                count += 1
              }

            }
            case other => getError("endTime error!")
          }
        }
        case other => getError("startTime error!")
      }
    } else {
      getError("need 3 params!")
    }

    if (statisArray == null)
      getError("donot have valid statisTime")

    statisArray
  }

  /**
   * Created by wangpf on 2017/6/28.
   * desc:打印错误并退出程序
   */
  def getError(msg: String) = {
    logError(msg)
    System.exit(0)
  }

  /**
   * Created by wangpf on 2017/6/30.
   * desc:扫描记录 并转成DataFrame，过滤掉不存在字段的数据
   */
  def scanAnalyToRdd(sc: SparkContext): RDD[(String, Long, Long, Long, Long)] = {
    // 创建hbase configuration
    hBaseConf.set(TableInputFormat.INPUT_TABLE, prop.getProperty("analyze_rst_tab"))
    hBaseConf.set(TableInputFormat.SCAN_COLUMN_FAMILY, "alarmChk")
    hBaseConf.set(TableInputFormat.SCAN_COLUMNS, "totalLogout_c_3g_cnt")
    hBaseConf.set(TableInputFormat.SCAN_COLUMNS, "totalLogout_c_4g_cnt")
    hBaseConf.set(TableInputFormat.SCAN_COLUMNS, "normalLogout_c_3g_cnt")
    hBaseConf.set(TableInputFormat.SCAN_COLUMNS, "normalLogout_c_4g_cnt")

    // 读取数据并转化成rdd
    val hBaseRDD = sc.newAPIHadoopRDD(hBaseConf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    // 将RDD转化为dataframe schema
    val shop = hBaseRDD.map( r => {
      val result = r._2

      (
        Bytes.toString(result.getRow),
        if (result.containsColumn(Bytes.toBytes("alarmChk"), Bytes.toBytes("totalLogout_c_3g_cnt")))
          Bytes.toString(result.getValue(Bytes.toBytes("alarmChk"), Bytes.toBytes("totalLogout_c_3g_cnt"))).toLong
        else
          0L,
        if (result.containsColumn(Bytes.toBytes("alarmChk"), Bytes.toBytes("totalLogout_c_4g_cnt")))
          Bytes.toString(result.getValue(Bytes.toBytes("alarmChk"), Bytes.toBytes("totalLogout_c_4g_cnt"))).toLong
        else
          0L,
        if (result.containsColumn(Bytes.toBytes("alarmChk"), Bytes.toBytes("normalLogout_c_3g_cnt")))
          Bytes.toString(result.getValue(Bytes.toBytes("alarmChk"), Bytes.toBytes("normalLogout_c_3g_cnt"))).toLong
        else
          0L,
        if (result.containsColumn(Bytes.toBytes("alarmChk"), Bytes.toBytes("normalLogout_c_4g_cnt")))
          Bytes.toString(result.getValue(Bytes.toBytes("alarmChk"), Bytes.toBytes("normalLogout_c_4g_cnt"))).toLong
        else
          0L
      )
    })

    shop
  }
}