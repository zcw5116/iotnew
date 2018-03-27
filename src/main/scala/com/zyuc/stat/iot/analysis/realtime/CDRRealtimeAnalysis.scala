package com.zyuc.stat.iot.analysis.realtime

import com.zyuc.stat.iot.analysis.util.{CDRHtableConverter, HbaseDataUtil}
import com.zyuc.stat.properties.ConfigProperties
import com.zyuc.stat.utils.{DateUtils, HbaseUtils}
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{Logging, SparkConf, SparkContext}

/**
  * 话单实时分析
  *
  * @author zhoucw
  * @version 1.0
  *
  */
object CDRRealtimeAnalysis extends Logging{

  /**
    * 主函数
    * 话单统计规则：
    * 1. 3g话单
    *     vpdn：pdsn清单中t8(source_ip_address)=0.0.0.0
    *     定向、普通：haccg
    * 2. 4g话单
    *     定向： 关联用户表含有定向标识的。
    *     vpdn： 关联用户表，排除定向， 限制用户表的属性为vpdn，用户的域名包含或者等于话单中的域名或者话单中包含private.vpdn/public.vpdn
    *     普通： 其他号码归属到普通业务
    * @param args
    */
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    sqlContext.sql("use " + ConfigProperties.IOT_HIVE_DATABASE)

    // 获取参数
    val appName = sc.getConf.get("spark.app.name","name_201710251500") // name_201708010040
    val userInfoTable = sc.getConf.get("spark.app.table.userInfoTable", "iot_basic_userinfo") //
    val userAndDomainTable = sc.getConf.get("spark.app.table.userAndDomainTable", "iot_basic_user_and_domain")
    val companyAndDomainTable = sc.getConf.get("spark.app.table.companyAndDomainTable", "iot_basic_company_and_domain")
    val userTableDataDayid = sc.getConf.get("spark.app.table.userTableDataDayid")

    val pdsnTable = sc.getConf.get("spark.app.table.pdsnTable", "iot_cdr_data_pdsn")
    val haccgTable = sc.getConf.get("spark.app.table.haccgTable", "iot_cdr_data_haccg")
    val pgwTable = sc.getConf.get("spark.app.table.pgwVPDNTable", "iot_cdr_data_pgw")
    val alarmHtablePre = sc.getConf.get("spark.app.htable.alarmTablePre", "analyze_summ_tab_flow_")
    val resultHtablePre = sc.getConf.get("spark.app.htable.resultHtablePre", "analyze_summ_rst_flow_")
    val resultDayHtable = sc.getConf.get("spark.app.htable.resultDayHtable", "analyze_summ_rst_everyday")
    val resultBSHtable = sc.getConf.get("spark.app.htable.resultBSHtable", "analyze_summ_rst_bs")
    val analyzeBPHtable = sc.getConf.get("spark.app.htable.analyzeBPHtable", "analyze_bp_tab")

    val vpnToApnMapFile = sc.getConf.get("spark.app.vpnToApnMapFile", "/hadoop/IOT/ANALY_PLATFORM/BasicData/VpdnToApn/vpdntoapn.txt")

    import sqlContext.implicits._

    val vpnToApnDF  = sqlContext.read.format("text").load(vpnToApnMapFile).map(x=>x.getString(0).split(",")).map(x=>(x(0),x(1))).toDF("vpdndomain","apn")
    val vpdnAndApnTable = "vpdnAndApnTable"
    vpnToApnDF.registerTempTable(vpdnAndApnTable)


    // 实时分析类型： 0-后续会离线重跑数据, 2-后续不会离线重跑数据
    val progRunType = sc.getConf.get("spark.app.progRunType", "0")
    // 距离当前历史同期的天数
    val hisDayNumStr = sc.getConf.get("spark.app.hisDayNums", "7")

    if(progRunType!="0" && progRunType!="1" ) {
      logError("param progRunType invalid, expect:0|1")
      return
    }
    var hisDayNum:Int = 0
    try {
      hisDayNum = hisDayNumStr.toInt
    }catch {
      case e:Exception => {
        logError("TypeConvert Failed. hisDayNumStr [" + hisDayNumStr + " cannot convert to Int ] ")
        return
      }
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

    // resultBSHtable
    val bsFamilies = new Array[String](1)
    bsFamilies(0) = "r"
    HbaseUtils.createIfNotExists(resultBSHtable, bsFamilies)

    ////////////////////////////////////////////////////////////////
    //   cache table
    ///////////////////////////////////////////////////////////////
    val userInfoTableCached = "userInfoTableCached"
    sqlContext.sql(s"cache table ${userInfoTableCached} as select mdn, imsicdma, companycode, vpdndomain, isvpdn, isdirect from $userInfoTable where d=$userTableDataDayid")

    //val userAndDomainTableCached = "userAndDomainTableCached"
    //sqlContext.sql(s"cache table ${userAndDomainTableCached} as select mdn, companycode, isvpdn, vpdndomain, apn, isdirect from $userAndDomainTable where d=$userTableDataDayid")


    // 关联3g的mdn, domain,  基站
    val mdnSql =
      s"""
         |select companycode, type, mdn, servtype, mdndomain as vpdndomain, bsid, downflow, upflow
         |from
         |(
         |select u.companycode, '3g' type, a.mdn,
         |       'C' as servtype,
         |       (case when array_contains(split(vpdndomain,','), regexp_replace(nai,'.*@','')) then regexp_replace(nai,'.*@','') else vpdndomain end) as mdndomains,
         |       a.bsid, a.termination as downflow, a.originating as upflow
         |from  ${pdsnTable} a, ${userInfoTableCached} u
         |where a.d = '${partitionD}'  and a.h = '${partitionH}' and a.m5='${partitionM5}'
         |      and a.source_ip_address='0.0.0.0'
         |      and a.mdn = u.mdn and u.isvpdn='1'
         |) t lateral view explode(split(t.mdndomains,',')) c as mdndomain
         |union all
         |select u.companycode, '3g' type, a.mdn,
         |       (case when u.isdirect=1 then 'D' else 'P' end) as servtype,
         |       '-1'  as vpdndomain,
         |       a.bsid, a.termination as downflow, a.originating as upflow
         |from ${haccgTable} a, ${userInfoTableCached} u
         |where a.d = '${partitionD}'  and a.h = '${partitionH}' and a.m5='${partitionM5}'
         |      and u.mdn = a.mdn
         |union all
         |select u.companycode, '4g' type, u.mdn,
         |(case when u.isdirect='1' then 'D' when u.isvpdn=1 and array_contains(split(u.vpdndomain,','), d.vpdndomain) then 'C' else 'P' end) as servtype,
         |(case when u.isdirect!='1' and array_contains(split(u.vpdndomain,','), d.vpdndomain) then d.vpdndomain else '-1' end)  as vpdndomain,
         |u.bsid, downflow, upflow
         |from
         |(
         |    select p.mdn,p.accesspointnameni as apn, t.companycode, t.vpdndomain, t.isdirect, t.isvpdn,p.bsid,
         |           nvl(l_datavolumefbcdownlink,0) as downflow, nvl(l_datavolumefbcuplink,0) as upflow
         |    from ${pgwTable} p, ${userInfoTableCached} t
         |    where p.mdn = t.mdn and p.d = '${partitionD}'  and p.h = '${partitionH}' and p.m5='${partitionM5}'
         |) u
         |left join ${vpdnAndApnTable} d
         |on(u.apn = d.apn and array_contains(split(u.vpdndomain,','),d.vpdndomain) )
       """.stripMargin


    val mdnTable = "mdnTable_" + startTime
    sqlContext.sql(mdnSql).registerTempTable(mdnTable)
    sqlContext.cacheTable(mdnTable)

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // 统计普通业务/定向业务/总的业务数据
    // servtype： 业务类型（D-定向， P-普通， C-VPDN）
    // vpdndomain： VPDN域名， 如果没有域名为-1
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
         |       sum(upflow) as upflow,
         |       sum(downflow) as downflow,
         |       avg(upflow) as avgupflow,
         |       avg(downflow) as avgdownflow,
         |       GROUPING__ID
         |from ${mdnTable}  m
         |group by companycode, servtype, vpdndomain, type
         |GROUPING SETS (companycode,(companycode, type),(companycode, servtype),(companycode, servtype, vpdndomain), (companycode, servtype, type), (companycode, servtype, vpdndomain, type))
       """.stripMargin

    // 对域名为-1的记录做过滤
    val statDF = sqlContext.sql(statSQL).filter("vpdndomain is null or vpdndomain!='-1'")

    /*
    statDF.filter("companycode='P100002368'").show
      +-----------+--------+---------------+----+-------+---------+------------+--------------+--------------+------------+
      |companycode|servtype|     vpdndomain|type|req_cnt|req_s_cnt|req_card_cnt|req_card_s_cnt|req_card_f_cnt|grouping__id|
      +-----------+--------+---------------+----+-------+---------+------------+--------------+--------------+------------+
      | P100002368|    null|           null|null|    412|      352|         286|           284|            23|           1|
      | P100002368|       P|           null|null|     62|        7|          24|             3|            21|           3|
      | P100002368|       C|           null|null|    350|      345|         264|           281|             2|           3|
      | P100002368|       C|fsgdjcb.vpdn.gd|null|    350|      345|         264|           281|             2|           7|
      | P100002368|    null|           null|  3g|    175|      170|         144|           144|             2|           9|
      | P100002368|    null|           null|  4g|    237|      182|         161|           140|            21|           9|
      | P100002368|       C|           null|  3g|    168|      163|         143|           141|             2|          11|
      | P100002368|       C|           null|  4g|    182|      182|         140|           140|             0|          11|
      | P100002368|       P|           null|  4g|     55|        0|          21|             0|            21|          11|
      | P100002368|       P|           null|  3g|      7|        7|           3|             3|             0|          11|
      | P100002368|       C|fsgdjcb.vpdn.gd|  4g|    182|      182|         140|           140|             0|          15|
      | P100002368|       C|fsgdjcb.vpdn.gd|  3g|    168|      163|         143|           141|             2|          15|
      +-----------+--------+---------------+----+-------+---------+------------+--------------+--------------+------------+
      */


    val resultRDD = statDF.repartition(13).rdd.map(x=>{
      val companyCode = if(null == x(0)) "-1" else x(0).toString
      val servType = if(null == x(1)) "-1" else x(1).toString
      val servFlag = if(servType == "D") "D" else if(servType == "C") "C"  else if(servType == "P") "P"  else "-1"
      val domain = if(null == x(2)) "-1" else x(2).toString
      val netType = if(null == x(3)) "-1" else x(3).toString
      val netFlag = if(netType=="3g") "3" else if(netType=="4g") "4"  else "t"
      val upflow = if(null == x(4)) "0" else  x(4).toString
      val downflow = if(null == x(5)) "0" else  x(5).toString
      val totalflow = (upflow.toDouble + downflow.toDouble).toString
      val avgupflow = if(null == x(6)) "0" else  x(6).toString
      val avgdownflow = if(null == x(7)) "0" else  x(7).toString
      val totalavgflow = (avgupflow.toDouble + avgdownflow.toDouble).toString

      val curAlarmRowkey = progRunType + "_" + dataTime.substring(8,12) + "_" + companyCode + "_" + servType + "_" + domain
      val curAlarmPut = new Put(Bytes.toBytes(curAlarmRowkey))
      curAlarmPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("f_c_" + netFlag + "_u"), Bytes.toBytes(upflow))
      curAlarmPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("f_c_" + netFlag + "_d"), Bytes.toBytes(downflow))
      curAlarmPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("f_c_" + netFlag + "_t"), Bytes.toBytes(totalflow))
      curAlarmPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("f_c_" + netFlag + "_upc"), Bytes.toBytes(avgupflow))
      curAlarmPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("f_c_" + netFlag + "_dpc"), Bytes.toBytes(avgdownflow))
      curAlarmPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("f_c_" + netFlag + "_tpc"), Bytes.toBytes(totalavgflow))

      val nextAlarmRowkey = progRunType + "_" + nextDataTime.substring(8,12) + "_" + companyCode + "_" + servType + "_" + domain
      val nexAlarmtPut = new Put(Bytes.toBytes(nextAlarmRowkey))
      nexAlarmtPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("f_p_" + netFlag + "_u"), Bytes.toBytes(upflow))
      nexAlarmtPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("f_p_" + netFlag + "_d"), Bytes.toBytes(downflow))
      nexAlarmtPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("f_p_" + netFlag + "_t"), Bytes.toBytes(totalflow))
      nexAlarmtPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("f_p_" + netFlag + "_upc"), Bytes.toBytes(avgupflow))
      nexAlarmtPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("f_p_" + netFlag + "_dpc"), Bytes.toBytes(avgdownflow))
      nexAlarmtPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("f_p_" + netFlag + "_tpc"), Bytes.toBytes(totalavgflow))

      val curResKey = companyCode +"_" + servType + "_" + domain + "_" + dataTime.substring(8,12)
      val curResPut = new Put(Bytes.toBytes(curResKey))
      curResPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("f_c_" + netFlag + "_u"), Bytes.toBytes(upflow))
      curResPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("f_c_" + netFlag + "_d"), Bytes.toBytes(downflow))
      curResPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("f_c_" + netFlag + "_t"), Bytes.toBytes(totalflow))
      curResPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("f_c_" + netFlag + "_upc"), Bytes.toBytes(avgupflow))
      curResPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("f_c_" + netFlag + "_dpc"), Bytes.toBytes(avgdownflow))
      curResPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("f_c_" + netFlag + "_tpc"), Bytes.toBytes(totalavgflow))

      val nextResKey = companyCode +"_" + servType + "_" + domain + "_" + nextDataTime.substring(8,12)
      val nextResPut = new Put(Bytes.toBytes(nextResKey))
      nextResPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("f_p_" + netFlag + "_u"), Bytes.toBytes(upflow))
      nextResPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("f_p_" + netFlag + "_d"), Bytes.toBytes(downflow))
      nextResPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("f_p_" + netFlag + "_t"), Bytes.toBytes(totalflow))
      nextResPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("f_p_" + netFlag + "_upc"), Bytes.toBytes(avgupflow))
      nextResPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("f_p_" + netFlag + "_dpc"), Bytes.toBytes(avgdownflow))
      nextResPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("f_p_" + netFlag + "_tpc"), Bytes.toBytes(totalavgflow))

      val dayResKey = dataTime.substring(2,8) + "_" + companyCode + "_" + servType + "_" + domain
      val dayResPut = new Put(Bytes.toBytes(dayResKey))
      dayResPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("f_bp_time"), Bytes.toBytes(dataTime.substring(2,8)))
      dayResPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("f_c_" + netFlag + "_u"), Bytes.toBytes(upflow))
      dayResPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("f_c_" + netFlag + "_d"), Bytes.toBytes(downflow))
      dayResPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("f_c_" + netFlag + "_t"), Bytes.toBytes(totalflow))
      dayResPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("f_c_" + netFlag + "_upc"), Bytes.toBytes(avgupflow))
      dayResPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("f_c_" + netFlag + "_dpc"), Bytes.toBytes(avgdownflow))
      dayResPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("f_c_" + netFlag + "_tpc"), Bytes.toBytes(totalavgflow))

      ((new ImmutableBytesWritable, curAlarmPut), (new ImmutableBytesWritable, nexAlarmtPut), (new ImmutableBytesWritable, curResPut), (new ImmutableBytesWritable, nextResPut), (new ImmutableBytesWritable, dayResPut))
    })


    HbaseDataUtil.saveRddToHbase(curAlarmHtable, resultRDD.map(x=>x._1))
    HbaseDataUtil.saveRddToHbase(nextAlarmHtable, resultRDD.map(x=>x._2))
    HbaseDataUtil.saveRddToHbase(curResultHtable, resultRDD.map(x=>x._3))
    HbaseDataUtil.saveRddToHbase(nextResultHtable, resultRDD.map(x=>x._4))
    HbaseDataUtil.saveRddToHbase(resultDayHtable, resultRDD.map(x=>x._5))


    val bsStatSQL =
      s"""
         |select companycode, servtype, vpdndomain, type, bsid,
         |       sum(upflow) as upflow,
         |       sum(downflow) as downflow,
         |       GROUPING__ID
         |from ${mdnTable}  m
         |group by companycode, servtype, vpdndomain, type, bsid
         |GROUPING SETS ((companycode, type, bsid),(companycode, servtype, type, bsid),(companycode, servtype, vpdndomain, type, bsid))
       """.stripMargin

    val bsResultDF = sqlContext.sql(bsStatSQL).filter("vpdndomain is null or vpdndomain!='-1'").coalesce(10)

    val bsResultRDD = bsResultDF.rdd.map(x=>{
      val c = if( null == x(0)) "-1" else x(0).toString // companycode
      val s = if(null == x(1)) "-1" else x(1).toString // servicetype
      val v = if(null == x(2)) "-1" else x(2).toString // vpdndomain
      val n = if(null == x(3)) "0" else x(3).toString //net
      val bid = if(null == x(4)) "0" else x(4).toString //enbid
      val uflow = if(null == x(5)) "0" else  x(5).toString // upflow
      val dflow = if(null == x(6)) "0" else   x(6).toString // downflow

      val rkey = dataTime + "_" + n + "_" + c + "_" + s + "_" + v + "_" + bid
      val put = new Put(Bytes.toBytes(rkey))
      put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("f_u"), Bytes.toBytes(uflow))
      put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("f_d"), Bytes.toBytes(dflow))
      (new ImmutableBytesWritable, put)
    })
    HbaseDataUtil.saveRddToHbase(resultBSHtable, bsResultRDD)


    ////////////////////////////////////////////////////////////////////////////////////////////////////////
    // 统计累积的流量
    //  对于每日00:00分的数据需要特殊处理， 在hbase里面00:00分的数据存储的是前一日23:55分至当日00:00分的数据
    ////////////////////////////////////////////////////////////////////////////////////////////////////////
    val preDataTime = DateUtils.timeCalcWithFormatConvertSafe(dataTime, "yyyyMMddHHmm", -5*60, "yyyyMMddHHmm")
    val curHbaseDF = CDRHtableConverter.convertToDF(sc, sqlContext, resultHtablePre + preDataTime.substring(0, 8))
    var resultDF = curHbaseDF.filter("time>='0005'")
    if(preDataTime.substring(0, 8) != dataTime.substring(0, 8)){
      val nextHbaseDF = CDRHtableConverter.convertToDF(sc, sqlContext, resultHtablePre + dataTime.substring(0, 8))
      if(nextHbaseDF!=null){
        resultDF = resultDF.unionAll(nextHbaseDF.filter("time='0000'"))
      }
    } else{
      resultDF = resultDF.filter("time<='" + dataTime.substring(8,12) + "'" )
    }
    val accumDF = resultDF.groupBy("compnyAndSerAndDomain").agg(sum("f_c_3_u").as("f_c_3_u"), sum("f_c_3_d").as("f_c_3_d"),
      sum("f_c_4_u").as("f_c_4_u"),sum("f_c_4_d").as("f_c_4_d"),
      sum("f_c_t_u").as("f_c_t_u"),sum("f_c_t_d").as("f_c_t_d"),
      sum("f_c_3_t").as("f_c_3_t"), sum("f_c_4_t").as("f_c_4_t"), sum("f_c_t_t").as("f_c_t_t"))

    val accumRDD = accumDF.repartition(10).rdd.map(x=>{
      val rkey = preDataTime.substring(2, 8) + "_" + x(0).toString
      val dayResPut = new Put(Bytes.toBytes(rkey))
      val f_c_3_u = x(1).toString
      val f_c_3_d = x(2).toString
      val f_c_4_u = x(3).toString
      val f_c_4_d = x(4).toString
      val f_c_t_u = x(5).toString
      val f_c_t_d = x(6).toString
      val f_c_3_t = x(7).toString
      val f_c_4_t = x(8).toString
      val f_c_t_t = x(9).toString

      dayResPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("f_d_3_u"), Bytes.toBytes(f_c_3_u))
      dayResPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("f_d_3_d"), Bytes.toBytes(f_c_3_d))
      dayResPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("f_d_3_t"), Bytes.toBytes(f_c_3_t))
      dayResPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("f_d_4_u"), Bytes.toBytes(f_c_4_u))

      dayResPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("f_d_4_d"), Bytes.toBytes(f_c_4_d))
      dayResPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("f_d_4_t"), Bytes.toBytes(f_c_4_t))
      dayResPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("f_d_t_u"), Bytes.toBytes(f_c_t_u))
      dayResPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("f_d_t_d"), Bytes.toBytes(f_c_t_d))
      dayResPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("f_d_t_t"), Bytes.toBytes(f_c_t_t))

      (new ImmutableBytesWritable, dayResPut)
    })

    HbaseDataUtil.saveRddToHbase(resultDayHtable, accumRDD)


    ////////////////////////////////////////////////////////////////////////////////////////////////////////
    //  统计历史同期的数据
    //
    ////////////////////////////////////////////////////////////////////////////////////////////////////////
    val hisDataTime = DateUtils.timeCalcWithFormatConvertSafe(dataTime, "yyyyMMddHHmm", -hisDayNum*24*60*60, "yyyyMMddHHmm")
    val hisDF = CDRHtableConverter.convertToDF(sc, sqlContext, resultHtablePre + hisDataTime.substring(0, 8)).filter("time='" + hisDataTime.substring(8, 12) + "'")

    if(hisDF != null){
      val hisResDF = hisDF.select("compnyAndSerAndDomain", "f_c_3_u", "f_c_3_d", "f_c_4_u",
        "f_c_4_d", "f_c_t_u", "f_c_t_d", "f_c_3_t", "f_c_4_t", "f_c_t_t")

     val hisResRDD = hisResDF.repartition(11).rdd.map(x=>{
        val rkey = dataTime.substring(2, 8) + "_" + x(0).toString
        val f_c_3_u = x(1).toString
        val f_c_3_d = x(2).toString
        val f_c_4_u = x(3).toString
        val f_c_4_d = x(4).toString
        val f_c_t_u = x(5).toString
        val f_c_t_d = x(6).toString
        val f_c_3_t = x(7).toString
        val f_c_4_t = x(8).toString
       val f_c_t_t = x(9).toString

        val dayResPut = new Put(Bytes.toBytes(rkey))
        dayResPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("f_h_3_u"), Bytes.toBytes(f_c_3_u))
        dayResPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("f_h_3_d"), Bytes.toBytes(f_c_3_d))
        dayResPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("f_h_4_u"), Bytes.toBytes(f_c_4_u))
        dayResPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("f_h_4_d"), Bytes.toBytes(f_c_4_d))
        dayResPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("f_h_t_u"), Bytes.toBytes(f_c_t_u))

        dayResPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("f_h_t_d"), Bytes.toBytes(f_c_t_d))
       dayResPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("f_h_3_t"), Bytes.toBytes(f_c_t_d))
       dayResPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("f_h_4_t"), Bytes.toBytes(f_c_t_d))
       dayResPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("f_h_u_t"), Bytes.toBytes(f_c_t_d))

        (new ImmutableBytesWritable, dayResPut)

      })

     // HbaseDataUtil.saveRddToHbase(resultDayHtable, hisResRDD)
    }



    // 更新时间, 断点时间比数据时间多1分钟
    val updateTime = DateUtils.timeCalcWithFormatConvertSafe(dataTime, "yyyyMMddHHmm", 1*60, "yyyyMMddHHmm")
    val analyzeColumn = if(progRunType == "0") "analyze_guess_bptime" else "analyze_real_bptime"
    HbaseUtils.upSertColumnByRowkey(analyzeBPHtable, "bp", "cdr", analyzeColumn, updateTime)


  }

}
