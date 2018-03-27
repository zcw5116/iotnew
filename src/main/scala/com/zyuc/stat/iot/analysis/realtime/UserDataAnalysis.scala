package com.zyuc.stat.iot.analysis.realtime

import com.zyuc.stat.iot.analysis.util.HbaseDataUtil
import com.zyuc.stat.properties.ConfigProperties
import com.zyuc.stat.utils.HbaseUtils
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{Logging, SparkConf, SparkContext}

/**
  * 用户数据， 跟终端表关联需要优化
  *
  * @author zhoucw
  * @version 1.0
  *
  */
object UserDataAnalysis extends Logging{

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
    val appName = sc.getConf.get("spark.app.name","name_20170801") // name_20170801
    val userInfoTable = sc.getConf.get("spark.app.table.userInfoTable", "iot_basic_userinfo") //
    val userAndDomainTable = sc.getConf.get("spark.app.table.userAndDomainTable", "iot_basic_user_and_domain")
    val companyAndDomainTable = sc.getConf.get("spark.app.table.companyAndDomainTable", "iot_basic_company_and_domain")
    val userTableDataDayid = sc.getConf.get("spark.app.table.userTableDataDayid", "20170922")

    val terminalTable = sc.getConf.get("spark.app.table.terminalTable", "iot_dim_terminal")

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
    sqlContext.sql(
      s"""
         |cache table ${userInfoTableCached} as
         |select u.mdn, (case when length(u.imei)=0 then '-1' else imei end) imei,
         |(case when t.devicetype in('Mobile Phone/Feature phone','Smartphone') then 0 else 1 end) ifnormal,
         | u.companycode, u.vpdndomain, u.isvpdn, u.isdirect, u.iscommon, t.devicetype, t.modelname
         |from $userInfoTable u left join ${terminalTable} t
         |on(substr(u.imei,0,8) = t.tac )  where d='$userTableDataDayid'
       """.stripMargin)


    val tmpDomainTable = "tmpDomainTable"
    sqlContext.sql(
      s"""
         |select companycode, servtype, domain as vpdndomain, imei, ifnormal
         |from(
         |    select u.companycode, 'C' as servtype,  vpdndomain, imei, ifnormal
         |    from ${userInfoTableCached} u
         |    where u.isvpdn='1'
         |) s lateral view explode(split(s.vpdndomain,',')) c as domain
       """.stripMargin).registerTempTable(tmpDomainTable)

    val userStatSQL =
      s"""
         |select u.companycode, 'C' as servtype,  '-1' as vpdndomain,
         |count(*) as usrcnt,
         |sum(case when u.imei='-1' then 0 else 1 end) as tercnt,
         |sum(case when u.ifnormal=0 then 1 else 0 end) as abnormalcnt
         |from ${userInfoTableCached} u
         |where u.isvpdn='1'
         |group by u.companycode
         |union all
         |select s.companycode, 'C' as servtype, domain as vpdndomain,
         |count(*) as usrcnt,
         |sum(case when s.imei='-1' then 0 else 1 end) as tercnt,
         |sum(case when s.ifnormal=0 then 1 else 0 end) as abnormalcnt
         |from
         |(   select u.companycode, 'C' as servtype,  vpdndomain, imei, ifnormal
         |    from ${userInfoTableCached} u
         |    where u.isvpdn='1'
         |) s lateral view explode(split(s.vpdndomain,',')) c as domain
         |group by s.companycode, domain
         |union all
         |select u.companycode, 'D' as servtype, '-1' as vpdndomain,
         |count(*) as usrcnt,
         |sum(case when u.imei='-1' then 0 else 1 end) as tercnt,
         |sum(case when u.ifnormal=0 then 1 else 0 end) as abnormalcnt
         |from ${userInfoTableCached} u
         |where u.isdirect='1'
         |group by u.companycode
         |union all
         |select u.companycode, 'P' as servtype, '-1' as vpdndomain,
         |count(*) as usrcnt,
         |sum(case when u.imei='-1' then 0 else 1 end) as tercnt,
         |sum(case when u.ifnormal=0 then 1 else 0 end) as abnormalcnt
         |from ${userInfoTableCached} u
         |where u.iscommon='1'
         |group by u.companycode
         |union all
         |select u.companycode, '-1' as servtype, '-1' as vpdndomain,
         |count(*) as usrcnt,
         |sum(case when u.imei='-1' then 0 else 1 end) as tercnt,
         |sum(case when u.ifnormal=0 then 1 else 0 end) as abnormalcnt
         |from ${userInfoTableCached} u
         |group by u.companycode
       """.stripMargin


    val userStatDF = sqlContext.sql(userStatSQL).coalesce(1)

    val userStatRDD = userStatDF.rdd.map(x=>{
      val comcode = if(null == x(0)) "-1" else x(0).toString
      val servtype = x(1).toString
      val vdomain = x(2).toString
      val usercnt = x(3).toString
      val tercnt = x(4).toString
      var abnormalcnt = x(5).toString

      val curResKey = dataTime.substring(2,8) + "_" + comcode + "_" + servtype + "_" + vdomain
      val curResPut = new Put(Bytes.toBytes(curResKey))
      curResPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("c_d_t_un"), Bytes.toBytes(usercnt))
      curResPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("c_d_t_tm"), Bytes.toBytes(tercnt))
      curResPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("c_d_t_abtm"), Bytes.toBytes(abnormalcnt))

      (new ImmutableBytesWritable, curResPut)
    })

    HbaseDataUtil.saveRddToHbase(resultDayHtable, userStatRDD)


    // 更新时间, 断点时间比数据时间多1分钟
    /*val updateTime = DateUtils.timeCalcWithFormatConvertSafe(dataTime, "yyyyMMddHHmm", 1*60, "yyyyMMddHHmm")
    val analyzeColumn = if(progRunType == "0") "analyze_guess_bptime" else "analyze_real_bptime"
    HbaseUtils.upSertColumnByRowkey(analyzeBPHtable, "bp", "msg", analyzeColumn, updateTime)*/


  }

}
