package com.zyuc.stat.iot.online

import com.zyuc.stat.properties.ConfigProperties
import com.zyuc.stat.utils.{DateUtils, HbaseUtils}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.{Logging, SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by zhoucw on 17-7-27.
  */
object OnlineUser extends Logging {

  // hbase配置
  val conf = HBaseConfiguration.create()
  conf.set("hbase.zookeeper.property.clientPort", ConfigProperties.IOT_ZOOKEEPER_CLIENTPORT)
  conf.set("hbase.zookeeper.quorum", ConfigProperties.IOT_ZOOKEEPER_QUORUM)


  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
    //.setMaster("local[4]").setAppName("fd")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    sqlContext.sql("use " + ConfigProperties.IOT_HIVE_DATABASE)
    val onlineType = sc.getConf.get("spark.app.onlineType") //"sum, detail"
    val m5timeid = sc.getConf.get("spark.app.m5timeid")
    val ifUpdatelarmFlag = sc.getConf.get("spark.app.ifUpdatelarmFlag","1")

    if(onlineType!="sum" && onlineType!="detail") {
      logError("onlineType para error, expected: sum, detail")
      return
    }

    var ifUpdatealarmChk = true
    if(ifUpdatelarmFlag=="0"){
      ifUpdatealarmChk = false
    } else {
      ifUpdatealarmChk = true
    }

    if(onlineType == "sum"){
      val baseHourid = HbaseUtils.getCloumnValueByRowkey("iot_dynamic_info","rowkey001","onlinebase","baseHourid") // 从habase里面获取
      // doOnlineDetail(sqlContext, m5timeid)
      doCalcOnlineNums(sc, sqlContext, m5timeid, baseHourid, ifUpdatealarmChk)
    } else if(onlineType == "detail") {
      val userTable = sc.getConf.get("spark.app.userTable") //"iot_customer_userinfo"
      var userTablePartitionID = DateUtils.timeCalcWithFormatConvertSafe(m5timeid.substring(0,8), "yyyyMMdd", -1*24*3600, "yyyyMMdd")
      userTablePartitionID = sc.getConf.get("spark.app.userTablePartitionDayID", userTablePartitionID)

      doOnlineDetail(sqlContext, m5timeid, userTable, userTablePartitionID)
    }






  }

  case class UserOnlineDetail(companycode: String, time: String, company_time: String, g3Start: String, g3Stop: String,
                              g4Start: String, g4Stop: String)

  def registerRDD(sc: SparkContext, htable: String): RDD[UserOnlineDetail] = {
    // 创建hbase configuration
    val hBaseConf = HBaseConfiguration.create()
    //hBaseConf.set("hbase.zookeeper.quorum","EPC-LOG-NM-15,EPC-LOG-NM-17,EPC-LOG-NM-16")
    hBaseConf.set("hbase.zookeeper.quorum", ConfigProperties.IOT_ZOOKEEPER_QUORUM)
    //设置zookeeper连接端口，默认2181
    //hBaseConf.set("hbase.zookeeper.property.clientPort", "2181")
    hBaseConf.set("hbase.zookeeper.property.clientPort", ConfigProperties.IOT_ZOOKEEPER_CLIENTPORT)

    hBaseConf.set(TableInputFormat.INPUT_TABLE, htable)
    // 从数据源获取数据
    val hbaseRDD = sc.newAPIHadoopRDD(hBaseConf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
    val radiusRDD = hbaseRDD.map(r => UserOnlineDetail(
      (Bytes.toString(r._2.getRow)).split("_")(0), (Bytes.toString(r._2.getRow)).split("_")(1),
      Bytes.toString(r._2.getRow),
      Bytes.toString(r._2.getValue(Bytes.toBytes("lineinfo"), Bytes.toBytes("3gStart"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("lineinfo"), Bytes.toBytes("3gStop"))),

      Bytes.toString(r._2.getValue(Bytes.toBytes("lineinfo"), Bytes.toBytes("4gStart"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("lineinfo"), Bytes.toBytes("4gStop")))

    ))
    radiusRDD
  }

  // m5timeid： 起始时间     baseHourid：用户在线基准数据的时间点
  def doCalcOnlineNums(sc: SparkContext, sqlContext: HiveContext, m5timeid: String, baseHourid: String, ifUpdatealarmChk:Boolean): Unit = {
    import sqlContext.implicits._

    //val m5timeid = "201707271600"
    //val baseHourid = "2017072700"
    // /hadoop/IOT/ANALY_PLATFORM/UserOnline/data/d=170727/h=00/

    // 计算时间点和基准数据时间点的间隔天数
    val beginDay = baseHourid.substring(0, 8)
    val endDay = m5timeid.substring(0, 8)
    val curHourM5 = m5timeid.substring(8, 12)

    val startMinuTime = baseHourid.substring(8, 10)+"00"

    val endMinuTime = m5timeid.substring(8, 12)

    // 获取间隔的天数
    val intervalDay = DateUtils.timeInterval(beginDay, endDay, "yyyyMMdd") / (24 * 60 * 60)

    var hbaseDF: DataFrame = null
    for (i <- 0 to intervalDay.toInt) {
      val dayid = DateUtils.timeCalcWithFormatConvertSafe(beginDay, "yyyyMMdd", i * 24 * 60 * 60, "yyyyMMdd")
      if (i == 0) {
        hbaseDF = registerRDD(sc, "iot_detail_online_users_" + dayid).toDF().filter(s"time >= ${startMinuTime}")
      } else if(i < intervalDay) {
        val tmpDF = registerRDD(sc, "iot_detail_online_users_" + dayid).toDF()
        hbaseDF = hbaseDF.unionAll(tmpDF)
      }

      if (i == intervalDay && intervalDay > 0) {
        val tmpDF = registerRDD(sc, "iot_detail_online_users_" + dayid).toDF().filter(s"time <= ${endMinuTime}")
        hbaseDF = hbaseDF.unionAll(tmpDF)
      }else if(intervalDay == 0){
        hbaseDF = hbaseDF.filter(s"time <= ${endMinuTime}")
      }

    }


    val hbaseTable = "iot_online_users_" + beginDay
    hbaseDF.registerTempTable(hbaseTable)

    val partionD = baseHourid.substring(2, 8)
    val partionH = baseHourid.substring(8, 10)
    val baseOnlineTable = "baseOnlineTable_" + beginDay
    val baseOnlineDF = sqlContext.sql(
      s"""
         |select vpdncompanycode, g3cnt, pgwcnt from iot_useronline_base_nums where d='${partionD}' and h='${partionH}'
       """.stripMargin)

    baseOnlineDF.registerTempTable(baseOnlineTable)

    val onlineDF = sqlContext.sql(
      s"""
         |select r.vpdncompanycode,
         |sum(g3cnt) as g3usernums, sum(pgwcnt) as g4usernums
         |from
         |(
         |    select h.companycode as vpdncompanycode,
         |           (nvl(h.g3Start,0) - nvl(h.g3Stop, 0)) as g3cnt,
         |           (nvl(h.g4Start,0) - nvl(h.g4Stop, 0)) as pgwcnt
         |    from ${hbaseTable} as h
         |    union all
         |    select b.vpdncompanycode, g3cnt, pgwcnt
         |    from ${baseOnlineTable} as b
         |) r
         |group by r.vpdncompanycode
       """.stripMargin).repartition(5)


    val htable = "iot_online_users_" + endDay
    // 如果h表不存在， 就创建
    val connection = ConnectionFactory.createConnection(conf)
    val families = new Array[String](1)
    families(0) = "info"

    // 创建表, 如果表存在， 自动忽略
    HbaseUtils.createIfNotExists(htable, families)
    val onlineJobConf = new JobConf(conf, this.getClass)
    onlineJobConf.setOutputFormat(classOf[TableOutputFormat])
    onlineJobConf.set(TableOutputFormat.OUTPUT_TABLE, htable)

    // 预警表
    val alarmChkTable = "analyze_rst_tab"
    val alarmChkJobConf = new JobConf(conf, this.getClass)
    alarmChkJobConf.setOutputFormat(classOf[TableOutputFormat])
    alarmChkJobConf.set(TableOutputFormat.OUTPUT_TABLE, alarmChkTable)

    val onlineHbaseRDD = onlineDF.rdd.map(x => (x.getString(0), x.getDouble(1), x.getDouble(2)))
    val onlineRDD = onlineHbaseRDD.map { arr => {
      val terminatePut = new Put(Bytes.toBytes(arr._1 + "_" + curHourM5.toString))
      val alarmPut = new Put(Bytes.toBytes(arr._1))

      terminatePut.addColumn(Bytes.toBytes("info"), Bytes.toBytes("c_3g_cnt"), Bytes.toBytes(arr._2.toString))
      terminatePut.addColumn(Bytes.toBytes("info"), Bytes.toBytes("c_4g_cnt"), Bytes.toBytes(arr._3.toString))
      terminatePut.addColumn(Bytes.toBytes("info"), Bytes.toBytes("c_all_cnt"),Bytes.toBytes((arr._2 + arr._3).toString))

      alarmPut.addColumn(Bytes.toBytes("alarmChk"), Bytes.toBytes("onlineuser_c_time"), Bytes.toBytes(m5timeid))
      alarmPut.addColumn(Bytes.toBytes("alarmChk"), Bytes.toBytes("onlineuser_c_cnt"), Bytes.toBytes((arr._2 + arr._3).toString))

      ((new ImmutableBytesWritable, terminatePut),(new ImmutableBytesWritable, alarmPut))
    }
    }
    onlineRDD.map(_._1).saveAsHadoopDataset(onlineJobConf)

    if(ifUpdatealarmChk){
      onlineRDD.map(_._2).saveAsHadoopDataset(alarmChkJobConf)
    }

  }


  def doOnlineDetail(sqlContext: HiveContext, m5timeid: String, userTable:String, userTablePartitionID:String) = {

    // val m5timeid = "201707280900"
    val startTime = m5timeid + "00" // 转换到s
    val endTime = DateUtils.timeCalcWithFormatConvertSafe(startTime, "yyyyMMddHHmmss", 5 * 60, "yyyyMMddHHmmss")
    val dayid = startTime.substring(0, 8)
    val curHourM5 = startTime.substring(8, 12)
    val nextHourM5 = endTime.substring(8, 12)


    val cachedUserinfoTable = "iot_user_basic_info_cached"
    val userDF = sqlContext.table(userTable).filter("d=" + userTablePartitionID).
      selectExpr("mdn", "custprovince", "case when length(vpdncompanycode)=0 then 'N999999999' else vpdncompanycode end  as vpdncompanycode")
    userDF.cache().registerTempTable(cachedUserinfoTable)


    val radiusTable = "radiusTable" + m5timeid
    sqlContext.sql(
      s"""
         |CACHE TABLE ${radiusTable} as
         |select mdn, status, nettype, regexp_replace(terminatecause, ' ', '') as terminatecause,
         |(case when terminatecause='UserRequest' then 'succ' else 'failed' end ) as result
         |from pgwradius_out where dayid='${dayid}' and time>='${startTime}'  and time<'${endTime}'
         |union all
         |select mdn, status, nettype, terminatecause,
         |(case when terminatecause in('2','7','8','9','11','12','13','14','15') then 'succ' else 'failed' end ) as result
         |from iot_radius_ha where dayid='${dayid}' and time>='${startTime}'  and time<'${endTime}'
       """.stripMargin).coalesce(1)


    val radiusSql =
      s"""
         |select u.vpdncompanycode,
         |sum(case when p.nettype='3G' and p.status='Start' then 1 else 0 end) as start3gCnt,
         |sum(case when p.nettype='3G' and p.status='Stop'  then 1 else 0 end) as start3gCnt,
         |sum(case when p.nettype='4G' and p.status='Start' then 1 else 0 end) as start4gCnt,
         |sum(case when p.nettype='4G' and p.status='Stop'  then 1 else 0 end) as start4gCnt
         |from ${radiusTable} p, ${cachedUserinfoTable} u
         |where u.mdn = p.mdn
         |group by u.vpdncompanycode
       """.stripMargin


    val radiusDF = sqlContext.sql(radiusSql).coalesce(1)

    val htable = "iot_detail_online_users_" + dayid
    // 如果h表不存在， 就创建
    val connection = ConnectionFactory.createConnection(conf)
    val families = new Array[String](2)
    families(0) = "lineinfo"
    families(1) = "termcase"

    // 创建表, 如果表存在， 自动忽略
    HbaseUtils.createIfNotExists(htable, families)
    val radiusJobConf = new JobConf(conf, this.getClass)
    radiusJobConf.setOutputFormat(classOf[TableOutputFormat])
    radiusJobConf.set(TableOutputFormat.OUTPUT_TABLE, htable)

    // type, vpdncompanycode, authcnt, successcnt, failedcnt, authmdnct, authfaieldcnt
    val curHbaserdd = radiusDF.rdd.map(x => (x.getString(0), x.getLong(1), x.getLong(2), x.getLong(3), x.getLong(4)))

    val curRDD = curHbaserdd.map { arr => {
      /*一个Put对象就是一行记录，在构造方法中指定主键
       * 所有插入的数据必须用org.apache.hadoop.hbase.util.Bytes.toBytes方法转换
       * Put.add方法接收三个参数：列族，列名，数据
       */
      val curPut = new Put(Bytes.toBytes(arr._1 + "_" + curHourM5.toString))
      curPut.addColumn(Bytes.toBytes("lineinfo"), Bytes.toBytes("3gStart"), Bytes.toBytes(arr._2.toString))
      curPut.addColumn(Bytes.toBytes("lineinfo"), Bytes.toBytes("3gStop"), Bytes.toBytes(arr._3.toString))
      curPut.addColumn(Bytes.toBytes("lineinfo"), Bytes.toBytes("4gStart"), Bytes.toBytes(arr._4.toString))
      curPut.addColumn(Bytes.toBytes("lineinfo"), Bytes.toBytes("4gStop"), Bytes.toBytes(arr._5.toString))

      val nextPut = new Put(Bytes.toBytes(arr._1 + "_" + nextHourM5.toString))
      nextPut.addColumn(Bytes.toBytes("lineinfo"), Bytes.toBytes("p_3gStart"), Bytes.toBytes(arr._2.toString))
      nextPut.addColumn(Bytes.toBytes("lineinfo"), Bytes.toBytes("p_3gStop"), Bytes.toBytes(arr._3.toString))
      nextPut.addColumn(Bytes.toBytes("lineinfo"), Bytes.toBytes("p_4gStart"), Bytes.toBytes(arr._4.toString))
      nextPut.addColumn(Bytes.toBytes("lineinfo"), Bytes.toBytes("p_4gStop"), Bytes.toBytes(arr._5.toString))

      //转化成RDD[(ImmutableBytesWritable,Put)]类型才能调用saveAsHadoopDataset
      ((new ImmutableBytesWritable, curPut), (new ImmutableBytesWritable, nextPut))
    }
    }
    curRDD.map(_._1).saveAsHadoopDataset(radiusJobConf)
    curRDD.map(_._2).saveAsHadoopDataset(radiusJobConf)

    val radiusTermiateSql =
      s"""
         |select u.vpdncompanycode, p.terminatecause,
         |sum(case when p.nettype='3G' then 1 else 0 end) as stopCause3GCnt,
         |sum(case when p.nettype='4G' then 1 else 0 end) as stopCause4GCnt
         |from ${radiusTable} p, ${cachedUserinfoTable} u
         |where u.mdn = p.mdn and p.status='Stop' and p.result<>'succ'
         |group by u.vpdncompanycode, p.terminatecause
       """.stripMargin
    val radiusTermiateDF = sqlContext.sql(radiusTermiateSql).coalesce(1)
    val termiateHbaseRDD = radiusTermiateDF.rdd.map(x => (x.getString(0), x.getString(1), x.getLong(2), x.getLong(3)))
    val terminateRDD = termiateHbaseRDD.map { arr => {
      val terminatePut = new Put(Bytes.toBytes(arr._1 + "_" + curHourM5.toString))
      terminatePut.addColumn(Bytes.toBytes("termcase"), Bytes.toBytes(arr._2 + "_3g_cnt"), Bytes.toBytes(arr._3.toString))
      terminatePut.addColumn(Bytes.toBytes("termcase"), Bytes.toBytes(arr._2 + "_4g_cnt"), Bytes.toBytes(arr._4.toString))
      (new ImmutableBytesWritable, terminatePut)
    }
    }
    terminateRDD.saveAsHadoopDataset(radiusJobConf)
  }

}
