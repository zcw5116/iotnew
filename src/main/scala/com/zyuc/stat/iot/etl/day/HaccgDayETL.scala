package com.zyuc.stat.iot.etl.day

import com.zyuc.stat.properties.ConfigProperties
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by liuzk on 18-5-29.
  */
object HaccgDayETL {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()//.setMaster("local[2]").setAppName("name_20180601")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)

    val appName = sc.getConf.get("spark.app.name")
    val inputPath = sc.getConf.get("spark.app.inputPath", "/user/iot_ete/data/cdr/transform/haccg/data")
    val outputPath = sc.getConf.get("spark.app.outputPath","/user/iot_ete/data/cdr/summ_d/haccg/")
    val userPath = sc.getConf.get("spark.app.userPath", "/user/iot/data/baseuser/data/")
    val userDataTime = sc.getConf.get("spark.app.userDataTime", "20180510")

    val dataTime = appName.substring(appName.lastIndexOf("_") + 1)
    val d = dataTime.substring(2, 8)
    val dayid = dataTime.substring(0, 8)
    val partitionPath = s"/d=$d/h=*/m5=*"

    val cdrTempTable = "haccgTempTable"
    sqlContext.read.format("orc").load(inputPath + partitionPath).filter("acct_status_type='2'")//新增结束 代表真实流量
      .selectExpr("mdn","sid","acce_province","acce_region","bsid","pdsn_address",  "originating as upflow",
        "termination as downflow","substr(meid,1,8) as tac","home_agent",
        "service_option", "acct_session_time", "number_of_active_transitions as times")//新增了service_option,时长,次数
      .registerTempTable(cdrTempTable)


    //3g : pdsn SID
    import sqlContext.implicits._
    val pdsnSIDPath = sc.getConf.get("spark.app.pdsnSIDPath", "/user/iot/data/basic/pdsnSID/pdsnSID.txt")
    val pdsnSIDTable = "pdsnSIDTable"
    sc.textFile(pdsnSIDPath).map(x=>x.split("\\t"))
      .map(x=>(x(0),x(1),x(2))).toDF("sid","provname","cityname").registerTempTable(pdsnSIDTable)
    // CRM
    val userDataPath = userPath + "/d=" + userDataTime
    val userDF = sqlContext.read.format("orc").load(userDataPath)//.filter("is3g='Y' or is4g='Y'")
      .selectExpr("mdn", "custid", "custname", "ind_type", "ind_det_type", "prodtype", "beloprov", "belocity")
    val tmpUserTable = "spark_tmpUser"
    userDF.registerTempTable(tmpUserTable)
    val userTable = "spark_User"
    sqlContext.sql(
      s"""
         |cache table ${userTable}
         |as
         |select mdn, custid, custname, ind_type, ind_det_type, prodtype, beloprov, belocity
         |from ${tmpUserTable}
       """.stripMargin)

    // 关联基本信息
    val mdnDF = sqlContext.sql(
      s"""
         |select  u.custid, u.custname, c.mdn, c.sid, b.provname as provid, nvl(b.cityname,'-') as lanid, c.bsid,
         |        c.pdsn_address as PDSNIP,
         |        u.ind_type as industry_level1, u.ind_det_type as industry_level2, u.prodtype as industry_form,
         |        u.beloprov as own_provid, u.belocity as own_lanid, c.tac as TerminalModel,
         |        c.upflow, c.downflow, c.home_agent as HAIP, c.service_option, c.acct_session_time, c.times
         |from ${cdrTempTable} c
         |inner join ${userTable} u on(c.mdn = u.mdn)
         |left join ${pdsnSIDTable} b on(c.sid = b.sid)
       """.stripMargin)

    val cdrMdnTable = "spark_cdrmdn"
    mdnDF.registerTempTable(cdrMdnTable)

    val resultDF = sqlContext.sql(
      s"""
         |select  custid, custname, mdn, sid, provid, lanid, bsid, PDSNIP, '-1' as apn,
         |        industry_level1, industry_level2, industry_form, own_provid, own_lanid, TerminalModel,
         |        HAIP, service_option,
         |        '-1' as busi, upflow, downflow, sessions, duration, times
         |from(
         |    select custid, custname, mdn, sid, provid, lanid, bsid, PDSNIP,
         |        industry_level1, industry_level2, industry_form, own_provid, own_lanid, TerminalModel,
         |        HAIP, service_option,
         |        sum(upflow) as upflow, sum(downflow) as downflow,
         |        count(mdn) as sessions, sum(acct_session_time) as duration, sum(times) as times
         |    from ${cdrMdnTable}
         |    group by custid, custname, mdn, sid, provid, lanid, bsid, PDSNIP,
         |        industry_level1, industry_level2, industry_form, own_provid, own_lanid, TerminalModel,
         |        HAIP, service_option
         |) t
       """.stripMargin)

    resultDF.repartition(10).write.mode(SaveMode.Overwrite).format("orc").save(outputPath + "dayid=" + dayid)

//    sqlContext.sql("use " + ConfigProperties.IOT_HIVE_DATABASE)
//    val partitonTable = "iot_stat_cdr_haccg_day"
//    val sql = s"alter table $partitonTable add IF NOT EXISTS partition(dayid='$dayid')"
//    sqlContext.sql(sql)

  }

}

