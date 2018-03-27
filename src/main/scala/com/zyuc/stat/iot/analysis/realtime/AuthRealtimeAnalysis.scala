package com.zyuc.stat.iot.analysis.realtime

import com.zyuc.stat.iot.analysis.util.{AuthHtableConverter, HbaseDataUtil}
import com.zyuc.stat.properties.ConfigProperties
import com.zyuc.stat.utils.{DateUtils, HbaseUtils, MathUtil}
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{Logging, SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.functions._

/**
  * 认证日志实时分析
  *
  * @author zhoucw
  * @version 1.0
  *
  */
object AuthRealtimeAnalysis extends Logging{

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
    val appName = sc.getConf.get("spark.app.name","name_201710230840") // name_201708010040
    val userInfoTable = sc.getConf.get("spark.app.table.userInfoTable", "iot_basic_userinfo") //
    val userAndDomainTable = sc.getConf.get("spark.app.table.userAndDomainTable", "iot_basic_user_and_domain")
    val companyAndDomainTable = sc.getConf.get("spark.app.table.companyAndDomainTable", "iot_basic_company_and_domain")
    val userTableDataDayid = sc.getConf.get("spark.app.table.userTableDataDayid")

    val auth3gTable = sc.getConf.get("spark.app.table.auth3gTable", "iot_userauth_3gaaa")
    val auth4gTable = sc.getConf.get("spark.app.table.auth4gTable", "iot_userauth_4gaaa")
    val authVPDNTable = sc.getConf.get("spark.app.table.authVPDNTable", "iot_userauth_vpdn")
    val alarmHtablePre = sc.getConf.get("spark.app.htable.alarmTablePre", "analyze_summ_tab_auth_")
    val resultHtablePre = sc.getConf.get("spark.app.htable.resultHtablePre", "analyze_summ_rst_auth_")
    val resultDayHtable = sc.getConf.get("spark.app.htable.resultDayHtable", "analyze_summ_rst_everyday")
    val resultBSHtable = sc.getConf.get("spark.app.htable.resultBSHtable", "analyze_summ_rst_bs")
    val analyzeBPHtable = sc.getConf.get("spark.app.htable.analyzeBPHtable", "analyze_bp_tab")


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
    val partitionD = startTime.substring(0, 8)
    val partitionH = startTime.substring(8, 10)

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
    sqlContext.sql(s"cache table ${userInfoTableCached} as select mdn, imsicdma, companycode, vpdndomain, isvpdn, isdirect, iscommon from $userInfoTable where d=$userTableDataDayid")

    //val userAndDomainTableCached = "userAndDomainTableCached"
    //sqlContext.sql(s"cache table ${userAndDomainTableCached} as select mdn, companycode, isvpdn, vpdndomain from $userAndDomainTable where d=$userTableDataDayid")

    // 关联3g的mdn, domain,  基站
    // 如果话单到号码没有走vpdn，mdndomain设置为-1
    val mdnSql =
      s"""
         |select u.companycode, m.type, m.mdn,
         |(case when u.isdirect=1 then 'D' when u.isvpdn=1 and array_contains(split(vpdndomain,','), m.mdndomain) then 'C' else 'P' end) as servtype,
         |(case when u.isdirect != 1  and u.isvpdn=1 and array_contains(split(vpdndomain,','), m.mdndomain) then m.mdndomain else '-1' end) as mdndomain,
         |auth_result, nasportid as bsid, result
         |from
         |(
         |select '3g' type, u.mdn,
         |       regexp_extract(a.nai_sercode, '^.+@(.*)', 1) as mdndomain,
         |       a.auth_result, a.nasportid,
         |       case when a.auth_result = 0 then 's' else 'f' end as result
         |from  ${userInfoTableCached} u, ${auth3gTable} a
         |where a.dayid = '${partitionD}'  and a.hourid = '${partitionH}'
         |      and u.imsicdma = a.imsicdma
         |      and a.auth_time >= '${startTimeStr}' and a.auth_time < '${endTimeStr}'
         |union all
         |select '4g' type, a.mdn,
         |       regexp_extract(a.nai_sercode, '^.+@(.*)', 1) as  mdndomain,
         |       a.auth_result, a.nasportid,
         |       case when a.auth_result = 0 then 's' else 'f' end as result
         |from ${auth4gTable} a
         |where a.dayid = '${partitionD}'  and a.hourid = '${partitionH}'
         |      and a.auth_time >= '${startTimeStr}' and a.auth_time < '${endTimeStr}'
         |union all
         |select 'vpdn' type, a.mdn,
         |       regexp_extract(a.nai_sercode, '^.+@(.*)', 1) as mdndomain,
         |       a.auth_result, "-1" as nasportid,
         |       case when a.auth_result = 1 then 's' else 'f' end as result
         |from ${authVPDNTable} a
         |where a.dayid = '${partitionD}'  and a.hourid = '${partitionH}'
         |      and a.auth_time >= '${startTimeStr}' and a.auth_time < '${endTimeStr}'
         |) m, ${userInfoTableCached} u
         |where m.mdn = u.mdn
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
         |       sum(reqcnt) as req_cnt,
         |       sum(reqsucccnt) as req_s_cnt,
         |       count(distinct mdn) as req_card_cnt,
         |       sum(case when result='s' then 1 else 0 end) as req_card_s_cnt,
         |       sum(case when result='f' then 1 else 0 end) as req_card_f_cnt,
         |       GROUPING__ID
         |from
         |(
         |    select t.companycode, t.servtype, t.mdndomain as vpdndomain, t.type, t.mdn, t.result,
         |           count(*) as reqcnt,
         |           sum(case when result='s' then 1 else 0 end) reqsucccnt
         |    from ${mdnTable} t
         |    group by t.companycode, t.servtype, t.mdndomain, t.type, t.mdn, t.result
         |) m
         |group by companycode, servtype, vpdndomain, type
         |GROUPING SETS (companycode,(companycode, type),(companycode, servtype),(companycode, servtype, vpdndomain), (companycode, servtype, type), (companycode, servtype, vpdndomain, type))
         |ORDER BY GROUPING__ID
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


    val resultRDD = statDF.repartition(10).rdd.map(x=>{
      val companyCode = x(0).toString
      val servType = if(null == x(1)) "-1" else x(1).toString
      val servFlag = if(servType == "D") "D" else if(servType == "C") "C"  else if(servType == "P") "P"  else "-1"
      val domain = if(null == x(2)) "-1" else x(2).toString
      val netType = if(null == x(3)) "-1" else x(3).toString
      val netFlag = if(netType=="3g") "3" else if(netType=="4g") "4" else if(netType=="vpdn") "v" else "t"
      val reqCnt = x(4).toString
      val reqSuccCnt = x(5).toString
      val reqCardCnt = x(6).toString
      val reqCardSuccCnt = x(7).toString
      val reqCardFailedCnt = x(8).toString


      val curAlarmRowkey = progRunType + "_" + dataTime.substring(8,12) + "_" + companyCode + "_" + servType + "_" + domain
      val curAlarmPut = new Put(Bytes.toBytes(curAlarmRowkey))
      curAlarmPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("a_c_" + netFlag + "_rn"), Bytes.toBytes(reqCnt))
      curAlarmPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("a_c_" + netFlag + "_sn"), Bytes.toBytes(reqSuccCnt))
      curAlarmPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("a_c_" + netFlag + "_rat"), Bytes.toBytes(MathUtil.divOpera(reqSuccCnt, reqCnt)))
      curAlarmPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("a_c_" + netFlag + "_rcn"), Bytes.toBytes(reqCardCnt))
      curAlarmPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("a_c_" + netFlag + "_scn"), Bytes.toBytes(reqCardSuccCnt))
      curAlarmPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("a_c_" + netFlag + "_fcn"), Bytes.toBytes(reqCardFailedCnt))

      val nextAlarmRowkey = progRunType + "_" + nextDataTime.substring(8,12) + "_" + companyCode + "_" + servType + "_" + domain
      val nexAlarmtPut = new Put(Bytes.toBytes(nextAlarmRowkey))
      nexAlarmtPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("a_p_" + netFlag + "_rn"), Bytes.toBytes(reqCnt))
      nexAlarmtPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("a_p_" + netFlag + "_sn"), Bytes.toBytes(reqSuccCnt))
      nexAlarmtPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("a_p_" + netFlag + "_rat"), Bytes.toBytes(MathUtil.divOpera(reqSuccCnt, reqCnt)))
      nexAlarmtPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("a_p_" + netFlag + "_rcn"), Bytes.toBytes(reqCardCnt))
      nexAlarmtPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("a_p_" + netFlag + "_scn"), Bytes.toBytes(reqCardSuccCnt))
      nexAlarmtPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("a_p_" + netFlag + "_fcn"), Bytes.toBytes(reqCardFailedCnt))

      val curResKey = companyCode +"_" + servType + "_" + domain + "_" + dataTime.substring(8,12)
      val curResPut = new Put(Bytes.toBytes(curResKey))
      curResPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("a_c_" + netFlag + "_rn"), Bytes.toBytes(reqCnt))
      curResPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("a_c_" + netFlag + "_sn"), Bytes.toBytes(reqSuccCnt))
      curResPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("a_c_" + netFlag + "_rat"), Bytes.toBytes(MathUtil.divOpera(reqSuccCnt, reqCnt)))
      curResPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("a_c_" + netFlag + "_rcn"), Bytes.toBytes(reqCardCnt))
      curResPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("a_c_" + netFlag + "_scn"), Bytes.toBytes(reqCardSuccCnt))
      curResPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("a_c_" + netFlag + "_fcn"), Bytes.toBytes(reqCardFailedCnt))

      val nextResKey = companyCode +"_" + servType + "_" + domain + "_" + nextDataTime.substring(8,12)
      val nextResPut = new Put(Bytes.toBytes(nextResKey))
      nextResPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("a_p_" + netFlag + "_rn"), Bytes.toBytes(reqCnt))
      nextResPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("a_p_" + netFlag + "_sn"), Bytes.toBytes(reqSuccCnt))
      nextResPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("a_p_" + netFlag + "_rat"), Bytes.toBytes(MathUtil.divOpera(reqSuccCnt, reqCnt)))
      nextResPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("a_p_" + netFlag + "_rcn"), Bytes.toBytes(reqCardCnt))
      nextResPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("a_p_" + netFlag + "_scn"), Bytes.toBytes(reqCardSuccCnt))
      nextResPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("a_p_" + netFlag + "_fcn"), Bytes.toBytes(reqCardFailedCnt))

      val dayResKey = dataTime.substring(2,8) + "_" + companyCode + "_" + servType + "_" + domain
      val dayResPut = new Put(Bytes.toBytes(dayResKey))
      dayResPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("a_bp_time"), Bytes.toBytes(dataTime.substring(2,8)))
      dayResPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("a_c_" + netFlag + "_rn"), Bytes.toBytes(reqCnt))
      dayResPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("a_c_" + netFlag + "_sn"), Bytes.toBytes(reqSuccCnt))
      dayResPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("a_c_" + netFlag + "_rat"), Bytes.toBytes(MathUtil.divOpera(reqSuccCnt, reqCnt)))
      dayResPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("a_c_" + netFlag + "_rcn"), Bytes.toBytes(reqCardCnt))
      dayResPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("a_c_" + netFlag + "_scn"), Bytes.toBytes(reqCardSuccCnt))
      dayResPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("a_c_" + netFlag + "_fcn"), Bytes.toBytes(reqCardFailedCnt))

      ((new ImmutableBytesWritable, curAlarmPut), (new ImmutableBytesWritable, nexAlarmtPut), (new ImmutableBytesWritable, curResPut), (new ImmutableBytesWritable, nextResPut), (new ImmutableBytesWritable, dayResPut))
    })


    HbaseDataUtil.saveRddToHbase(curAlarmHtable, resultRDD.map(x=>x._1))
    HbaseDataUtil.saveRddToHbase(nextAlarmHtable, resultRDD.map(x=>x._2))
    HbaseDataUtil.saveRddToHbase(curResultHtable, resultRDD.map(x=>x._3))
    HbaseDataUtil.saveRddToHbase(nextResultHtable, resultRDD.map(x=>x._4))
    HbaseDataUtil.saveRddToHbase(resultDayHtable, resultRDD.map(x=>x._5))


    ////////////////////////////////////////////////////////////////////////////////////////////////////////
    //   基站数据
    //
    ////////////////////////////////////////////////////////////////////////////////////////////////////////
/*    val bsSQL =
      s"""
         |select companycode, servtype, mdndomain, bsid,
         |       count(*) as rn,
         |       sum(case when result='s' then 1 else 0 end) rsn
         |from ${mdnTable}  where type='3g'
         |group by companycode, servtype, mdndomain, bsid
         |grouping sets((companycode, servtype, bsid), (companycode, servtype, mdndomain, bsid))
       """.stripMargin*/

    val bsSQL =
      s"""
         |select companycode, servtype, mdndomain, bsid,
         |       sum(reqcnt) as rn,
         |       sum(reqfailcnt) as rfn,
         |       sum(reqsucccnt) as rsn,
         |       count(distinct mdn) as req_card_cnt,
         |       sum(case when result='s' then 0 else 1 end) req_card_fcnt,
         |       sum(case when result='s' then 1 else 0 end) req_card_scnt
         |from
         |(
         |     select t.companycode, t.servtype, t.mdndomain, t.type, t.bsid, t.mdn, t.result,
         |            count(*) as reqcnt,
         |            sum(case when result='s' then 1 else 0 end) reqsucccnt,
         |            sum(case when result='s' then 0 else 1 end) reqfailcnt
         |     from   ${mdnTable} t
         |     group by t.companycode, t.servtype, t.mdndomain, t.type, t.bsid, t.mdn, t.result
         |) m
         |where type='3g'
         |group by companycode, servtype, mdndomain, bsid
         |grouping sets((companycode, servtype, bsid), (companycode, servtype, mdndomain, bsid))
     """.stripMargin


    val bsResultDF = sqlContext.sql(bsSQL).filter("mdndomain is null or mdndomain!='-1'").coalesce(10)

    val bsResultRDD = bsResultDF.rdd.map(x=>{
      val c = if( null == x(0)) "-1" else x(0).toString // companycode
      val s = if(null == x(1)) "-1" else x(1).toString // servicetype
      val v = if(null == x(2)) "-1" else x(2).toString // vpdndomain
      val bid = if(null == x(3)) "0" else x(3).toString //enbid为空的置为0
      val rn = x(4).toString  // reqcnt
      val rfn = x(5).toString // request failed cnt
      val rsn = x(6).toString // request success cnt
      val rcn = x(7).toString  // req_card_cnt
      val rfcn = x(8).toString // req card_failed cnt
      val rscn = x(9).toString // req card_sucess cnt

      val rkey = dataTime + "_3g_" + c + "_" + s + "_" + v + "_" + bid
      val put = new Put(Bytes.toBytes(rkey))
      put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("a_rn"), Bytes.toBytes(rn))
      put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("a_fn"), Bytes.toBytes(rfn))
      put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("a_sn"), Bytes.toBytes(rsn))
      put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("a_rcn"), Bytes.toBytes(rcn))
      put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("a_fcn"), Bytes.toBytes(rfcn))
      put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("a_scn"), Bytes.toBytes(rscn))
      (new ImmutableBytesWritable, put)
    })
    HbaseDataUtil.saveRddToHbase(resultBSHtable, bsResultRDD)




    ////////////////////////////////////////////////////////////////////////////////////////////////////////
    //   失败原因写入
    //
    ////////////////////////////////////////////////////////////////////////////////////////////////////////

    val failedSQL =
      s"""
         |select companycode, servtype, mdndomain, type, auth_result as errcode,
         |count(*) errCnt,
         |GROUPING__ID
         |from ${mdnTable} t where t.result = 'f'
         |group by companycode, servtype, mdndomain, type, auth_result
         |GROUPING SETS ((companycode,auth_result),(companycode, type,auth_result),(companycode, servtype,auth_result),(companycode, servtype, mdndomain,auth_result), (companycode, servtype, type,auth_result), (companycode, servtype, mdndomain, type,auth_result))
         |ORDER BY GROUPING__ID
       """.stripMargin

    val failedDF = sqlContext.sql(failedSQL).filter("mdndomain is null or mdndomain!='-1'").coalesce(1)
    val failedRDD = failedDF.repartition(10).rdd.map(x=>{
      val companyCode = x(0).toString
      val servType = if(null == x(1)) "-1" else x(1).toString
      val servFlag = if(servType == "D") "D" else if(servType == "C") "C"  else if(servType == "P") "P"  else "-1"
      val domain = if(null == x(2)) "-1" else x(2).toString
      val netType = if(null == x(3)) "-1" else x(3).toString
      val netFlag = if(netType=="3g") "3" else if(netType=="4g") "4" else if(netType=="vpdn") "v" else "t"
      val errcode = if(null == x(4)) "-1" else x(4).toString
      val errCnt = x(5).toString
      val curResKey = companyCode +"_" + servType + "_" + domain + "_" + dataTime.substring(8,12)
      val curAlarmRowkey = progRunType + "_" + dataTime.substring(8,12) + "_" + companyCode + "_" + servType + "_" + domain

      val curResPut = new Put(Bytes.toBytes(curResKey))
      val curAlarmPut = new Put(Bytes.toBytes(curResKey))
      curResPut.addColumn(Bytes.toBytes("e"), Bytes.toBytes("a_" + netFlag + "_" + errcode + "_cnt"), Bytes.toBytes(errCnt))
      curAlarmPut.addColumn(Bytes.toBytes("e"), Bytes.toBytes("a_" + netFlag + "_" + errcode + "_cnt"), Bytes.toBytes(errCnt))

      ((new ImmutableBytesWritable, curResPut),(new ImmutableBytesWritable, curAlarmPut))
    })
    HbaseDataUtil.saveRddToHbase(curResultHtable, failedRDD.map(_._1))
    HbaseDataUtil.saveRddToHbase(curResultHtable, failedRDD.map(_._2))


    ////////////////////////////////////////////////////////////////////////////////////////////////////////
    // 统计累积的认证用户数
    //  对于每日00:00分的数据需要特殊处理， 在hbase里面00:00分的数据存储的是前一日23:55分至当日00:00分的数据
    ////////////////////////////////////////////////////////////////////////////////////////////////////////
    val preDataTime = DateUtils.timeCalcWithFormatConvertSafe(dataTime, "yyyyMMddHHmm", -5*60, "yyyyMMddHHmm")
    val curHbaseDF = AuthHtableConverter.convertToDF(sc, sqlContext, resultHtablePre + preDataTime.substring(0, 8))

    var resultDF = curHbaseDF.filter("time>='0005'")
    if(preDataTime.substring(0, 8) != dataTime.substring(0, 8)){
      val nextHbaseDF = AuthHtableConverter.convertToDF(sc, sqlContext, resultHtablePre + dataTime.substring(0, 8))
      if(nextHbaseDF!=null){
        resultDF = resultDF.unionAll(nextHbaseDF.filter("time='0000'"))
      }
    } else{
      resultDF = resultDF.filter("time<='" + dataTime.substring(8,12) + "'" )
    }
    val accumDF = resultDF.groupBy("compnyAndSerAndDomain").agg(sum("a_c_3_rn").as("req_3g_sum"), sum("a_c_3_sn").as("req_3g_succ_sum"),
      sum("a_c_4_rn").as("req_4g_sum"),sum("a_c_4_sn").as("req_4g_succ_sum"),
      sum("a_c_v_rn").as("req_vpdn_sum"),sum("a_c_v_sn").as("req_vpdn_succ_sum"),
      sum("a_c_t_rn").as("req_total_sum"),sum("a_c_t_sn").as("req_total_succ_sum"))

    val accumRDD = accumDF.repartition(8).rdd.map(x=>{
      val rkey = preDataTime.substring(2, 8) + "_" + x(0).toString
      val dayResPut = new Put(Bytes.toBytes(rkey))
      val req_3g_sum = x(1).toString
      val req_succ_3g_sum  = x(2).toString
      val req_4g_sum = x(3).toString
      val req_succ_4g_sum = x(4).toString
      val req_vpdn_sum = x(5).toString
      val req_succ_vpdn_sum = x(6).toString
      val req_total_sum = x(7).toString
      val req_succ_total_sum = x(8).toString
      dayResPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("a_d_3_rn"), Bytes.toBytes(req_3g_sum))
      dayResPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("a_d_3_sn"), Bytes.toBytes(req_succ_3g_sum))
      dayResPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("a_d_3_rat"), Bytes.toBytes(MathUtil.divOpera(req_succ_3g_sum, req_3g_sum )))

      dayResPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("a_d_4_rn"), Bytes.toBytes(req_4g_sum))
      dayResPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("a_d_4_sn"), Bytes.toBytes(req_succ_4g_sum))
      dayResPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("a_d_4_rat"), Bytes.toBytes(MathUtil.divOpera(req_succ_4g_sum, req_4g_sum )))

      dayResPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("a_d_v_rn"), Bytes.toBytes(req_vpdn_sum))
      dayResPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("a_d_v_rn"), Bytes.toBytes(req_succ_vpdn_sum))
      dayResPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("a_d_v_rat"), Bytes.toBytes(MathUtil.divOpera(req_succ_vpdn_sum, req_vpdn_sum )))

      dayResPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("a_d_t_rn"), Bytes.toBytes(req_total_sum))
      dayResPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("a_d_t_sn"), Bytes.toBytes(req_succ_total_sum))
      dayResPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("a_d_t_rat"), Bytes.toBytes(MathUtil.divOpera(req_succ_total_sum, req_total_sum )))

      (new ImmutableBytesWritable, dayResPut)
    })

    HbaseDataUtil.saveRddToHbase(resultDayHtable, accumRDD)





    // 更新时间, 断点时间比数据时间多1分钟
    val updateTime = DateUtils.timeCalcWithFormatConvertSafe(dataTime, "yyyyMMddHHmm", 1*60, "yyyyMMddHHmm")
    val analyzeColumn = if(progRunType == "0") "analyze_guess_bptime" else "analyze_real_bptime"
    HbaseUtils.upSertColumnByRowkey(analyzeBPHtable, "bp", "auth", analyzeColumn, updateTime)


  }

}
