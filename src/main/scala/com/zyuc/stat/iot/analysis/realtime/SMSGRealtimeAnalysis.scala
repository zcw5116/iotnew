package com.zyuc.stat.iot.analysis.realtime

import com.zyuc.stat.iot.analysis.util.{HbaseDataUtil, SMSGHtableConverter}
import com.zyuc.stat.properties.ConfigProperties
import com.zyuc.stat.utils.{DateUtils, HbaseUtils, MathUtil}
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{Logging, SparkConf, SparkContext}

/**
  * 认证日志实时分析
  *
  * @author zhoucw
  * @version 1.0
  *
  */
object SMSGRealtimeAnalysis extends Logging{

  /**
    * 主函数
    * @param args
    */
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    sqlContext.sql("use " + ConfigProperties.IOT_HIVE_DATABASE)

    // 获取参数
    val appName = sc.getConf.get("spark.app.name","name_201708010040") // name_201708010040
    val userInfoTable = sc.getConf.get("spark.app.table.userInfoTable", "iot_basic_userinfo") //
    val userAndDomainTable = sc.getConf.get("spark.app.table.userAndDomainTable", "iot_basic_user_and_domain")
    val companyAndDomainTable = sc.getConf.get("spark.app.table.companyAndDomainTable", "iot_basic_company_and_domain")
    val userTableDataDayid = sc.getConf.get("spark.app.table.userTableDataDayid", "20170922")

    val smsglogTable = sc.getConf.get("spark.app.table.smsgTable", "iot_cdr_data_smsc")

    val alarmHtablePre = sc.getConf.get("spark.app.htable.alarmTablePre", "analyze_summ_tab_msg_")
    val resultHtablePre = sc.getConf.get("spark.app.htable.resultHtablePre", "analyze_summ_rst_msg_")
    val resultDayHtable = sc.getConf.get("spark.app.htable.resultDayHtable", "analyze_summ_rst_everyday")
    val analyzeBPHtable = sc.getConf.get("spark.app.htable.analyzeBPHtable", "analyze_bp_tab")


    // 实时分析类型： 0-后续会离线重跑数据, 2-后续不会离线重跑数据
    val progRunType = sc.getConf.get("spark.app.progRunType", "0")


    if(progRunType!="0" && progRunType!="1" ) {
      logError("param progRunType invalid, expect:0|1")
      return
    }


    //  dataTime-当前数据时间  nextDataTime-下一个时刻数据的时间
    val dataTime = appName.substring(appName.lastIndexOf("_") + 1)
    val nextDataTime = DateUtils.timeCalcWithFormatConvertSafe(dataTime, "yyyyMMddHHmm", 5*60, "yyyyMMddHHmm")
    // 转换成hive表中的时间格式
    val startTimeStr = DateUtils.timeCalcWithFormatConvertSafe(dataTime, "yyyyMMddHHmm", -5*60, "yyyy-MM-dd HH:mm:ss")
    val endTimeStr = DateUtils.timeCalcWithFormatConvertSafe(dataTime, "yyyyMMddHHmm", 0, "yyyy-MM-dd HH:mm:ss")
    // 转换成hive表中的分区字段值
    val startTime =  DateUtils.timeCalcWithFormatConvertSafe(dataTime, "yyyyMMddHHmm", -5*60, "yyyyMMddHHmm")
    val partitionD = startTime.substring(2, 8)
    val partitionH = startTime.substring(8, 10)
    val partitionM5 = startTime.substring(10, 12)

    /////////////////////////////////////////////////////////////////////////////////////////
    //  Hbase 相关的表
    //  表不存在， 就创建
    /////////////////////////////////////////////////////////////////////////////////////////
    //  curAlarmHtable-当前时刻的预警表,  nextAlarmHtable-下一时刻的预警表,
    val curAlarmHtable = alarmHtablePre + dataTime.substring(0,8)
    val nextAlarmHtable = alarmHtablePre + nextDataTime.substring(0,8)
    val alarmFamilies = new Array[String](2)
    alarmFamilies(0) = "s"
    alarmFamilies(1) = "e"
    // 创建表, 如果表存在， 自动忽略
    HbaseUtils.createIfNotExists(curAlarmHtable,alarmFamilies)
    HbaseUtils.createIfNotExists(nextAlarmHtable,alarmFamilies)

    //  curResultHtable-当前时刻的结果表,  nextResultHtable-下一时刻的结果表
    val curResultHtable = resultHtablePre + dataTime.substring(0,8)
    val nextResultHtable = resultHtablePre + nextDataTime.substring(0,8)
    val resultFamilies = new Array[String](2)
    resultFamilies(0) = "s"
    resultFamilies(1) = "e"
    HbaseUtils.createIfNotExists(curResultHtable, resultFamilies)
    HbaseUtils.createIfNotExists(nextResultHtable, resultFamilies)

    // resultDayHtable
    val resultDayFamilies = new Array[String](1)
    resultDayFamilies(0) = "s"
    HbaseUtils.createIfNotExists(resultDayHtable, resultDayFamilies)

    // analyzeBPHtable
    val analyzeBPFamilies = new Array[String](1)
    analyzeBPFamilies(0) = "bp"
    HbaseUtils.createIfNotExists(analyzeBPHtable, analyzeBPFamilies)


    ////////////////////////////////////////////////////////////////
    //   cache table
    ///////////////////////////////////////////////////////////////
    val userInfoTableCached = "userInfoTableCached"
    sqlContext.sql(s"cache table ${userInfoTableCached} as select mdn, imsicdma, companycode, vpdndomain, isvpdn, isdirect, iscommon from $userInfoTable where d=$userTableDataDayid")

    //val userAndDomainTableCached = "userAndDomainTableCached"
    //sqlContext.sql(s"cache table ${userAndDomainTableCached} as select mdn, companycode, isvpdn, vpdndomain from $userAndDomainTable where d=$userTableDataDayid")

    // 关联  type: r-receive, s-send
    val mdnSql =
    s"""
       |select u.mdn, u.companycode, u.vpdndomain, 'r' type, u.isdirect, u.isvpdn, u.iscommon
       |from  ${userInfoTableCached} u, ${smsglogTable} m
       |where m.d = '${partitionD}'  and m.h = '${partitionH}' and m.m5='${partitionM5}'
       |      and u.mdn = m.called_number
       |union all
       |select u.mdn, u.companycode, u.vpdndomain, 's' type, u.isdirect, u.isvpdn, u.iscommon
       |from  ${userInfoTableCached} u, ${smsglogTable} m
       |where m.d = '${partitionD}'  and m.h = '${partitionH}' and m.m5='${partitionM5}'
       |      and u.mdn = m.calling_number
       """.stripMargin


    val mdnTable = "mdnTable_" + startTime
    sqlContext.sql(mdnSql).registerTempTable(mdnTable)
    sqlContext.cacheTable(mdnTable)



    //////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // 统计普通业务/定向业务/总的业务数据
    // servtype： 业务类型（D-定向， P-普通， C-VPDN）
    // vpdndomain： VPDN域名， 如果没有域名为-1， 需要过滤
    // type:  网络类型
    // req_cnt: 请求数
    // req_s_cnt: 请求成功数
    // req_card_cnt: 请求卡数
    // req_card_s_cnt: 请求成功卡数
    // req_card_f_cnt: 请求失败卡数
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////
    val statSQL =
    s"""
       |select companycode, servtype, vpdndomain, type,
       |       sum(reqcnt) as reqcnt,
       |       count(distinct mdn) as reqcardcnt
       |from
       |(
       |    select t.companycode, 'D' as servtype, "-1" as vpdndomain, t.mdn, t.type,
       |           count(*) as reqcnt
       |    from ${mdnTable} t where t.isdirect='1'
       |    group by t.companycode, t.vpdndomain, t.mdn, t.type
       |    union all
       |    select companycode, servtype, c.vpdndomain, mdn, type, reqcnt
       |    from
       |    (
       |         select t.companycode, 'C' as servtype, t.vpdndomain, t.mdn, t.type,
       |                count(*) as reqcnt
       |        from ${mdnTable} t where t.isvpdn='1'
       |        group by t.companycode, t.vpdndomain, t.mdn, t.type
       |    ) s lateral view explode(split(s.vpdndomain,',')) c as vpdndomain
       |    union all
       |    select t.companycode, 'C' as servtype, '-1' as vpdndomain, t.mdn, t.type,
       |           count(*) as reqcnt
       |    from ${mdnTable} t where t.isvpdn='1'
       |    group by t.companycode, t.vpdndomain, t.mdn, t.type
       |    union all
       |    select t.companycode, 'P' as servtype, "-1" as vpdndomain, t.mdn, t.type,
       |           count(*) as reqcnt
       |    from ${mdnTable} t where t.iscommon='1'
       |    group by t.companycode, t.vpdndomain, t.mdn, t.type
       |    union all
       |     select t.companycode, '-1' as servtype, "-1" as vpdndomain, t.mdn, t.type,
       |           count(*) as reqcnt
       |    from ${mdnTable} t
       |    group by t.companycode, t.vpdndomain, t.mdn, t.type
       |) m
       |group by companycode, servtype, vpdndomain, type
       |GROUPING SETS ((companycode, servtype, vpdndomain), (companycode, servtype, vpdndomain, type))
       """.stripMargin

    // 对域名为-1的记录做过滤
    val statDF = sqlContext.sql(statSQL).coalesce(1)

    /*
    scala> statDF.filter("companycode='P100002368'").show
          +-----------+--------+---------------+-------+---------+------------+--------------+--------------+------------+
          |companycode|servtype|     vpdndomain|req_cnt|req_s_cnt|req_card_cnt|req_card_s_cnt|req_card_f_cnt|grouping__id|
          +-----------+--------+---------------+-------+---------+------------+--------------+--------------+------------+
          | P100002368|       P|           null|     82|       25|          55|            21|            42|           3|
          | P100002368|       C|           null|     82|       25|          55|            21|            42|           3|
          | P100002368|    null|           null|    164|       50|          55|            42|            84|           1|
          | P100002368|       C|fsgdjcb.vpdn.gd|     82|       25|          55|            21|            42|           7|
          +-----------+--------+---------------+-------+---------+------------+--------------+--------------+------------+
      */


    val resultRDD = statDF.rdd.map(x=>{
      val companyCode = x(0).toString
      val servType = if(null == x(1)) "-1" else x(1).toString
      val servFlag = if(servType == "D") "D" else if(servType == "C") "C"  else if(servType == "P") "P"  else "-1"
      val domain = if(null == x(2)) "-1" else x(2).toString
      val sendType = if(null == x(3)) "-1" else x(3).toString
      val sendFlag = if(sendType == "s") "s" else if(sendType == "r") "r" else "t"
      val reqCnt = x(4).toString
      val reqCardCnt = x(5).toString

      val curAlarmRowkey = progRunType + "_" + dataTime.substring(8,12) + "_" + companyCode + "_" + servType + "_" + domain
      val curAlarmPut = new Put(Bytes.toBytes(curAlarmRowkey))
      curAlarmPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("ms_c_" + sendFlag + "_n"), Bytes.toBytes(reqCnt))
      curAlarmPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("ms_c_" + sendFlag + "_cn"), Bytes.toBytes(reqCardCnt))

      val nextAlarmRowkey = progRunType + "_" + nextDataTime.substring(8,12) + "_" + companyCode + "_" + servType + "_" + domain
      val nexAlarmtPut = new Put(Bytes.toBytes(nextAlarmRowkey))
      nexAlarmtPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("ms_p_" + sendFlag + "_n"), Bytes.toBytes(reqCnt))
      nexAlarmtPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("ms_p_" + sendFlag + "_cn"), Bytes.toBytes(reqCardCnt))

      val curResKey = companyCode +"_" + servType + "_" + domain + "_" + dataTime.substring(8,12)
      val curResPut = new Put(Bytes.toBytes(curResKey))
      curResPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("ms_c_" + sendFlag + "_n"), Bytes.toBytes(reqCnt))
      curResPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("ms_c_" + sendFlag + "_cn"), Bytes.toBytes(reqCardCnt))

      val nextResKey = companyCode +"_" + servType + "_" + domain + "_" + nextDataTime.substring(8,12)
      val nextResPut = new Put(Bytes.toBytes(nextResKey))
      nextResPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("ms_p_" + sendFlag + "_n"), Bytes.toBytes(reqCnt))
      nextResPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("ms_p_" + sendFlag + "_cn"), Bytes.toBytes(reqCardCnt))

      val dayResKey = dataTime.substring(2,8) + "_" + companyCode + "_" + servType + "_" + domain
      val dayResPut = new Put(Bytes.toBytes(dayResKey))
      dayResPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("ms_c_" + sendFlag + "_n"), Bytes.toBytes(reqCnt))
      dayResPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("ms_c_" + sendFlag + "_cn"), Bytes.toBytes(reqCardCnt))

      ((new ImmutableBytesWritable, curAlarmPut), (new ImmutableBytesWritable, nexAlarmtPut), (new ImmutableBytesWritable, curResPut), (new ImmutableBytesWritable, nextResPut), (new ImmutableBytesWritable, dayResPut))
    })


    HbaseDataUtil.saveRddToHbase(curAlarmHtable, resultRDD.map(x=>x._1))
    HbaseDataUtil.saveRddToHbase(nextAlarmHtable, resultRDD.map(x=>x._2))
    HbaseDataUtil.saveRddToHbase(curResultHtable, resultRDD.map(x=>x._3))
    HbaseDataUtil.saveRddToHbase(nextResultHtable, resultRDD.map(x=>x._4))
    HbaseDataUtil.saveRddToHbase(resultDayHtable, resultRDD.map(x=>x._5))

    

    ////////////////////////////////////////////////////////////////////////////////////////////////////////
    // 统计累积的认证用户数
    //  对于每日00:00分的数据需要特殊处理， 在hbase里面00:00分的数据存储的是前一日23:55分至当日00:00分的数据
    ////////////////////////////////////////////////////////////////////////////////////////////////////////
    val preDataTime = DateUtils.timeCalcWithFormatConvertSafe(dataTime, "yyyyMMddHHmm", -5*60, "yyyyMMddHHmm")
    val curHbaseDF = SMSGHtableConverter.convertToDF(sc, sqlContext, resultHtablePre + preDataTime.substring(0, 8))
    var resultDF = curHbaseDF.filter("time>='0005'")
    if(preDataTime.substring(0, 8) != dataTime.substring(0, 8)){
      val nextHbaseDF = SMSGHtableConverter.convertToDF(sc, sqlContext, resultHtablePre + dataTime.substring(0, 8))
      if(nextHbaseDF!=null){
        resultDF = resultDF.unionAll(nextHbaseDF.filter("time='0000'"))
      }
    } else{
      resultDF = resultDF.filter("time<='" + dataTime.substring(8,12) + "'" )
    }
    val accumDF = resultDF.groupBy("compnyAndSerAndDomain").agg(
      sum("ms_c_r_n").as("ms_c_r_n"),sum("ms_c_s_n").as("ms_c_s_n"),sum("ms_c_t_n").as("ms_c_t_n"))

    val accumRDD = accumDF.repartition(10).rdd.map(x=>{
      val rkey = preDataTime.substring(2, 8) + "_" + x(0).toString
      val dayResPut = new Put(Bytes.toBytes(rkey))
      val ms_c_r_n = x(1).toString
      val ms_c_s_n = x(2).toString
      val ms_c_t_n = x(3).toString

      dayResPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("ms_d_r_n"), Bytes.toBytes(ms_c_r_n))
      dayResPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("ms_d_s_n"), Bytes.toBytes(ms_c_s_n))
      dayResPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("ms_d_t_n"), Bytes.toBytes(ms_c_t_n))

      (new ImmutableBytesWritable, dayResPut)
    })

    HbaseDataUtil.saveRddToHbase(resultDayHtable, accumRDD)



    // 更新时间, 断点时间比数据时间多1分钟
    val updateTime = DateUtils.timeCalcWithFormatConvertSafe(dataTime, "yyyyMMddHHmm", 1*60, "yyyyMMddHHmm")
    val analyzeColumn = if(progRunType == "0") "analyze_guess_bptime" else "analyze_real_bptime"
    HbaseUtils.upSertColumnByRowkey(analyzeBPHtable, "bp", "msg", analyzeColumn, updateTime)


  }

}
