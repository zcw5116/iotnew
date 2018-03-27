package com.zyuc.stat.iot.user

import java.io.InputStream
import java.text.SimpleDateFormat
import java.util.Calendar

import com.zyuc.stat.tools.GetProperties
import com.zyuc.stat.utils.{CommonUtils, HbaseUtils}
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.filter.{RegexStringComparator, CompareFilter, RowFilter}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.io.Source

/**
 * Created by wangpf on 2017/6/28.
 * desc:计算出截止到统计时间的每个企业的用户数
 */
object IotOnlineUsernum extends GetProperties {
  override def inputStreamArray: Array[InputStream] = Array(
    this.getClass.getClassLoader.getResourceAsStream("kafka.proerties")
  )

  // 获取配置文件的内容
  private val prop = props

  def main(args: Array[String]) {
    // 处理参数
    val fileName = "/home/slview/test/wangpf/BPFile/IotOnlineUsernum.BP"
    // 处理的数据时间不大于获取增量数据的时间
    val IotUserLineDetailfileName = "/home/slview/test/wangpf/BPFile/IotUserLineDetail.BP"
    val (statisTime, referTime, bpTime, flag) = getStatisTime(fileName, IotUserLineDetailfileName)
    val statisDay = statisTime.substring(0, 8)
    val statisHhmm = statisTime.substring(8, 12)

    // 每天凌晨根据基准在线用户数和上下线信息进行计算
    if (flag == 0) {
      // 创建表
      createTbaleIfexists(statisDay)
      // 创建上下文
      val sparkConf = new SparkConf()
        .setAppName("IotOnlineUsernum")

      val sc = new SparkContext(sparkConf)

      // 创建hiveContext
      val hiveContext = new HiveContext(sc)

      // 获取在线用户数基准数据
      hiveContext
        .read
        .format("orc")
        .load(s"/hadoop/IOT/ANALY_PLATFORM/UserOnline/${referTime}/part*")
        .toDF("companycode", "3gonlineusernum", "4gonlineusernum")
        .registerTempTable("UserOnline")

      // 获取hbase上下线数据
      scanRecordToDf(sc, hiveContext, "iot_detail_online_users_" + referTime)
        .registerTempTable("iot_detail_online_users_info")

      // 计算当前时间在线用户数
      val sql =
        "select " +
          " if(uo.companycode is null, idoui.companycode, uo.companycode), " +
          " if(uo.3gonlineusernum is null, 0, uo.3gonlineusernum) + if(idoui.3gusernum is null, 0, idoui.3gusernum), " +
          " if(uo.4gonlineusernum is null, 0, uo.4gonlineusernum) + if(idoui.4gusernum is null, 0, idoui.4gusernum) " +
          "from UserOnline uo " +
          "full outer join " +
          " (select companycode, " +
          "   sum(3gStart - 3gStop) 3gusernum, " +
          "   sum(4gStart - 4gStop) 4gusernum " +
          "  from iot_detail_online_users_info " +
          " group by companycode) idoui " +
          "on uo.companycode = idoui.companycode"

      println("sql = " + sql)

      // 获取当前基线数据
      val conn = HbaseUtils.getConnect(prop.getProperty("hbase.zookeeper.quorum"),
        prop.getProperty("hbase.zookeeper.property.clientPort"))
      val currBaselineData = getBaselineData(conn, "iot_online_users_" + statisDay, statisHhmm)
      val referData = referRecordFilter(conn, "iot_online_users_" + referTime, "2355")

      // 获取hbasejob
      val hBaseConf = HbaseUtils.getHbaseConf("EPC-LOG-NM-15,EPC-LOG-NM-17,EPC-LOG-NM-16", "2181")
      val jobconf = HbaseUtils.getHbasejobConf(hBaseConf, "iot_online_users_" + statisDay)
      val jobconf2 = HbaseUtils.getHbasejobConf(hBaseConf, "analyze_rst_tab")
      // 批量插入hbase
      val data = hiveContext.sql(sql).coalesce(1).rdd.map( x => {
        val put = new Put(Bytes.toBytes(x(0) + "_" + statisHhmm))
        //为put操作指定 column 和 value
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("c_3g_cnt"), Bytes.toBytes(x(1).toString))
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("c_4g_cnt"), Bytes.toBytes(x(2).toString))

        // 插入分析数据表
        val refervalue = getMapDataLong(referData, x(0).toString)
        val currBaselinevalue = getMapDataLong(currBaselineData, x(0).toString)
        val put2 = new Put(Bytes.toBytes(x(0) + "_" + statisHhmm))
        //为put操作指定 column 和 value
        put2.addColumn(Bytes.toBytes("alarmChk"), Bytes.toBytes("onlineuser_c_3g_time"), Bytes.toBytes(statisDay + statisHhmm))
        put2.addColumn(Bytes.toBytes("alarmChk"), Bytes.toBytes("onlineuser_c_3g_cnt"), Bytes.toBytes(x(1).toString))
        put2.addColumn(Bytes.toBytes("alarmChk"), Bytes.toBytes("onlineuser_b_3g_cnt"), Bytes.toBytes(currBaselinevalue._1.toString))
        put2.addColumn(Bytes.toBytes("alarmChk"), Bytes.toBytes("onlineuser_p_3g_cnt"), Bytes.toBytes(refervalue._1.toString))

        put2.addColumn(Bytes.toBytes("alarmChk"), Bytes.toBytes("onlineuser_c_4g_time"), Bytes.toBytes(statisDay + statisHhmm))
        put2.addColumn(Bytes.toBytes("alarmChk"), Bytes.toBytes("onlineuser_c_4g_cnt"), Bytes.toBytes(x(2).toString))
        put2.addColumn(Bytes.toBytes("alarmChk"), Bytes.toBytes("onlineuser_b_4g_cnt"), Bytes.toBytes(currBaselinevalue._2.toString))
        put2.addColumn(Bytes.toBytes("alarmChk"), Bytes.toBytes("onlineuser_p_4g_cnt"), Bytes.toBytes(refervalue._2.toString))

        ((new ImmutableBytesWritable, put), (new ImmutableBytesWritable, put2))
      })

      data.persist()
      data.map(_._1).saveAsHadoopDataset(jobconf)
      data.map(_._2).saveAsHadoopDataset(jobconf2)
      data.unpersist()

      conn.close()
      sc.stop()
    }
    // 其他时间根据上一个时间的在线用户数和上下信息进行计算
    else {
      // 获取上一个时间点的在线用户数
      val referTimeDay = referTime.substring(0, 8)
      val referTimeHhmm = referTime.substring(8, 12)

      println(referTimeDay + "-----" + referTimeHhmm)
      println(statisDay + "-----" + statisHhmm)

      val conn = HbaseUtils.getConnect(prop.getProperty("hbase.zookeeper.quorum"),
        prop.getProperty("hbase.zookeeper.property.clientPort"))

      val referData = referRecordFilter(conn, "iot_online_users_" + referTimeDay, referTimeHhmm)
      // 上下信息进行也是使用上个时间点
      val StatisData = StatisRecordFilter(conn, "iot_detail_online_users_" + referTimeDay, referTimeHhmm)

      // 找到所有的company
      val companyMap = scala.collection.mutable.Map[String, Int]()
      referData.keys.foreach( companycode => companyMap(companycode) = 1)
      StatisData.keys.foreach( companycode => companyMap(companycode) = 1)

      // 获取当前基线数据
      val currBaselineData = getBaselineData(conn, "iot_online_users_" + statisDay, statisHhmm)

      // 数据入库
      val table1 = conn.getTable(TableName.valueOf("iot_online_users_" + statisDay))
      val table2 = conn.getTable(TableName.valueOf("analyze_rst_tab"))
      companyMap.keys.foreach( companycode => {
        //        println(companycode)
        val refervalue = getMapDataLong(referData, companycode)
        val Statisvalue = getMapDataLong(StatisData, companycode)
        val currBaselinevalue = getMapDataLong(currBaselineData, companycode)

        val onlineuser_c_3g_cnt = (refervalue._1 + Statisvalue._1).toString
        val onlineuser_c_4g_cnt = (refervalue._2 + Statisvalue._2).toString
        // 插入在线用户数据
        val put1 = new Put(Bytes.toBytes(companycode + "_" + statisHhmm))
        //为put操作指定 column 和 value
        put1.addColumn(Bytes.toBytes("info"), Bytes.toBytes("c_3g_cnt"), Bytes.toBytes(onlineuser_c_3g_cnt))
        put1.addColumn(Bytes.toBytes("info"), Bytes.toBytes("c_4g_cnt"), Bytes.toBytes(onlineuser_c_4g_cnt))

        table1.put(put1)
        // 插入分析数据表
        val put2 = new Put(Bytes.toBytes(companycode))
        //为put操作指定 column 和 value
        put2.addColumn(Bytes.toBytes("alarmChk"), Bytes.toBytes("onlineuser_c_3g_time"), Bytes.toBytes(statisDay + statisHhmm))
        put2.addColumn(Bytes.toBytes("alarmChk"), Bytes.toBytes("onlineuser_c_3g_cnt"), Bytes.toBytes(onlineuser_c_3g_cnt))
        put2.addColumn(Bytes.toBytes("alarmChk"), Bytes.toBytes("onlineuser_b_3g_cnt"), Bytes.toBytes(currBaselinevalue._1.toString))
        put2.addColumn(Bytes.toBytes("alarmChk"), Bytes.toBytes("onlineuser_p_3g_cnt"), Bytes.toBytes(refervalue._1.toString))

        put2.addColumn(Bytes.toBytes("alarmChk"), Bytes.toBytes("onlineuser_c_4g_time"), Bytes.toBytes(statisDay + statisHhmm))
        put2.addColumn(Bytes.toBytes("alarmChk"), Bytes.toBytes("onlineuser_c_4g_cnt"), Bytes.toBytes(onlineuser_c_4g_cnt))
        put2.addColumn(Bytes.toBytes("alarmChk"), Bytes.toBytes("onlineuser_b_4g_cnt"), Bytes.toBytes(currBaselinevalue._2.toString))
        put2.addColumn(Bytes.toBytes("alarmChk"), Bytes.toBytes("onlineuser_p_4g_cnt"), Bytes.toBytes(refervalue._2.toString))

        table2.put(put2)
      })

      table1.close()
      table2.close()
      conn.close()
    }

    // 更新BPTime
    CommonUtils.updateBptime(fileName, bpTime)
  }

  /**
   * Created by wangpf on 2017/6/28.
   * desc:将不存在的数据置为0
   */
  private def getMapDataLong(map: mutable.Map[String, (Long, Long)], key: String): (Long, Long) = {
    if (map.contains(key))
      map(key)
    else
      (0L, 0L)
  }

  /**
   * Created by wangpf on 2017/6/28.
   * desc:根据参数获取处理时间
   */
  private def getStatisTime(fileName: String, IotUserLineDetailfileName: String): (String, String, String, Int) = {
    var referTime: String = null
    var bpTime: String = null
    var flag: Int = 0
    // 从文件中获取参数
    val source = Source.fromFile(fileName)
    val statisTime = source.getLines.next()
    source.close

    val IotUserLineDetailsource = Source.fromFile(IotUserLineDetailfileName)
    val IotUserLineDetailTime = IotUserLineDetailsource.getLines.next()
    IotUserLineDetailsource.close

    if (statisTime > IotUserLineDetailTime) {
      println("IotUserLineDetail is slow!")
      System.exit(0)
    } else {
      val m = "[0-9]{12}".r
      statisTime match {
        case m() => {
          val sdf = new SimpleDateFormat("yyyyMMddHHmm")
          // 获取基准值的时间
          val inDate = sdf.parse(statisTime)
          val calendar = Calendar.getInstance()
          // 获取BPTime
          calendar.setTime(inDate)
          calendar.add(Calendar.MINUTE, 5)
          bpTime = sdf.format(calendar.getTime())

          if (statisTime.substring(8, 12) == "0000") {
            calendar.setTime(inDate)
            calendar.add(Calendar.HOUR, -24)
            referTime = sdf.format(calendar.getTime()).substring(0, 8)
          } else {
            calendar.setTime(inDate)
            calendar.add(Calendar.MINUTE, -5)
            referTime = sdf.format(calendar.getTime())
            flag = 1
          }
        }
        case other => {
          println("param error!")
          System.exit(0)
        }
      }
    }

    println("statisTime: " + statisTime)
    println("referTime: " + referTime)
    println("flag: " + flag)
    (statisTime, referTime, bpTime, flag)
  }

  /**
   * Created by wangpf on 2017/6/28.
   * desc:创建hbase表
   */
  private def createTbaleIfexists(statisDay: String): Unit = {
    val conn = HbaseUtils.getConnect("EPC-LOG-NM-15,EPC-LOG-NM-17,EPC-LOG-NM-16", "2181")
    //Hbase表模式管理器
    val admin = conn.getAdmin
    //本例将操作的表名
    val tableName = TableName.valueOf("iot_online_users_" + statisDay)
    //如果需要创建表
    if (!admin.tableExists(tableName)) {
      //创建Hbase表模式
      val tableDescriptor = new HTableDescriptor(tableName)
      //创建列簇1
      tableDescriptor.addFamily(new HColumnDescriptor("info".getBytes()))
      //创建表
      admin.createTable(tableDescriptor)
      println("create done.")
    }
    conn.close()
  }

  /**
   * Created by wangpf on 2017/6/28.
   * desc:模糊匹配出数据转为DataFrame
   */
  private def scanRecordToDf(sc: SparkContext, sqlContext: SQLContext, tablename: String): DataFrame ={
    // 创建hbase configuration
    val hBaseConf = HbaseUtils.getHbaseConf("EPC-LOG-NM-15,EPC-LOG-NM-17,EPC-LOG-NM-16", "2181")
    hBaseConf.set(TableInputFormat.INPUT_TABLE, tablename)
    hBaseConf.set(TableInputFormat.SCAN_COLUMN_FAMILY, "lineinfo")

    // 进行隐式转换
    import sqlContext.implicits._

    // 读取数据并转化成rdd
    val hBaseRDD = sc.newAPIHadoopRDD(hBaseConf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    // 将RDD转化为dataframe schema
    val shop = hBaseRDD.map(r => (
      Bytes.toString(r._2.getRow).split("_")(0),
      if (r._2.containsColumn(Bytes.toBytes("lineinfo"), Bytes.toBytes("3gStart")))
        Bytes.toString(r._2.getValue(Bytes.toBytes("lineinfo"), Bytes.toBytes("3gStart"))).toLong
      else
        0L,
      if (r._2.containsColumn(Bytes.toBytes("lineinfo"), Bytes.toBytes("3gStop")))
        Bytes.toString(r._2.getValue(Bytes.toBytes("lineinfo"), Bytes.toBytes("3gStop"))).toLong
      else
        0L,
      if (r._2.containsColumn(Bytes.toBytes("lineinfo"), Bytes.toBytes("4gStart")))
        Bytes.toString(r._2.getValue(Bytes.toBytes("lineinfo"), Bytes.toBytes("4gStart"))).toLong
      else
        0L,
      if (r._2.containsColumn(Bytes.toBytes("lineinfo"), Bytes.toBytes("4gStop")))
        Bytes.toString(r._2.getValue(Bytes.toBytes("lineinfo"), Bytes.toBytes("4gStop"))).toLong
      else
        0L
      ))
      .toDF("companycode", "3gStart", "3gStop", "4gStart", "4gStop")

    shop
  }

  /**
   * Created by wangpf on 2017/6/28.
   * desc:模糊匹配出数据转为Map
   */
  private def referRecordFilter(connection: Connection, tablename: String, Hhmm: String): mutable.Map[String, (Long, Long)] ={
    val dataMap = scala.collection.mutable.Map[String, (Long, Long)]()

    val userTable = TableName.valueOf(tablename)
    val table = connection.getTable(userTable)
    val s = new Scan()
    val filter = new RowFilter(CompareFilter.CompareOp.EQUAL,
      new RegexStringComparator(("^.*" + Hhmm+ "$")))
    s.setFilter(filter)
    s.addColumn("info".getBytes(), "c_3g_cnt".getBytes())
    s.addColumn("info".getBytes(), "c_4g_cnt".getBytes())
    val scanner = table.getScanner(s)
    var result: Result = scanner.next()
    while(result != null) {
      dataMap(Bytes.toString(result.getRow).split("_")(0)) =
        (Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("c_3g_cnt"))).toLong,
          Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("c_4g_cnt"))).toLong)

      result = scanner.next()
    }
    table.close()
    scanner.close()

    dataMap
  }

  /**
   * Created by wangpf on 2017/6/28.
   * desc:模糊匹配出数据转为Map
   */
  private def StatisRecordFilter(connection: Connection, tablename: String, Hhmm: String): mutable.Map[String, (Long, Long)] ={
    val dataMap = scala.collection.mutable.Map[String, (Long, Long)]()

    val userTable = TableName.valueOf(tablename)
    val table = connection.getTable(userTable)
    val s = new Scan()
    val filter = new RowFilter(CompareFilter.CompareOp.EQUAL,
      new RegexStringComparator(("^.*" + Hhmm+ "$")))
    s.setFilter(filter)
    s.addColumn("lineinfo".getBytes(), "3gStart".getBytes())
    s.addColumn("lineinfo".getBytes(), "3gStop".getBytes())
    s.addColumn("lineinfo".getBytes(), "4gStart".getBytes())
    s.addColumn("lineinfo".getBytes(), "4gStop".getBytes())
    val scanner = table.getScanner(s)
    var result: Result = scanner.next()
    while(result != null) {
      val value_3gStart =
        if (result.containsColumn(Bytes.toBytes("lineinfo"), Bytes.toBytes("3gStart")))
          Bytes.toString(result.getValue(Bytes.toBytes("lineinfo"), Bytes.toBytes("3gStart"))).toLong
        else
          0L
      val value_3gStop =
        if (result.containsColumn(Bytes.toBytes("lineinfo"), Bytes.toBytes("3gStop")))
          Bytes.toString(result.getValue(Bytes.toBytes("lineinfo"), Bytes.toBytes("3gStop"))).toLong
        else
          0L
      val value_4gStart =
        if (result.containsColumn(Bytes.toBytes("lineinfo"), Bytes.toBytes("4gStart")))
          Bytes.toString(result.getValue(Bytes.toBytes("lineinfo"), Bytes.toBytes("4gStart"))).toLong
        else
          0L
      val value_4gStop =
        if (result.containsColumn(Bytes.toBytes("lineinfo"), Bytes.toBytes("4gStop")))
          Bytes.toString(result.getValue(Bytes.toBytes("lineinfo"), Bytes.toBytes("4gStop"))).toLong
        else
          0L

      dataMap(Bytes.toString(result.getRow).split("_")(0)) = (value_3gStart - value_3gStop, value_4gStart - value_4gStop)

      result = scanner.next()
    }
    table.close()
    scanner.close()

    dataMap
  }

  /**
   * Created by wangpf on 2017/6/28.
   * desc:获取当前统计时间的基线数据
   */
  private def getBaselineData(connection: Connection, tablename: String, Hhmm: String): mutable.Map[String, (Long, Long)] = {
    val dataMap = scala.collection.mutable.Map[String, (Long, Long)]()

    val userTable = TableName.valueOf(tablename)
    val table = connection.getTable(userTable)
    val s = new Scan()
    val filter = new RowFilter(CompareFilter.CompareOp.EQUAL,
      new RegexStringComparator(("^.*" + Hhmm+ "$")))
    s.setFilter(filter)
    s.addColumn("info".getBytes(), "b_3g_cnt".getBytes())
    s.addColumn("info".getBytes(), "b_4g_cnt".getBytes())
    val scanner = table.getScanner(s)
    var result: Result = scanner.next()
    while(result != null) {
      dataMap(Bytes.toString(result.getRow).split("_")(0)) =
        (Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("b_3g_cnt"))).toLong,
          Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("b_4g_cnt"))).toLong)

      result = scanner.next()
    }
    table.close()
    scanner.close()

    dataMap
  }
}
