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
    sqlContext.read.format("orc").load(inputPath + partitionPath)
      .selectExpr("mdn","siteid","acce_province","acce_region","bsid","pdsn_address",  "originating as upflow",
        "termination as downflow","substr(meid,1,8) as tac","home_agent", "service_option")//新增了service_option
      .registerTempTable(cdrTempTable)


    // 基站
    val iotBSInfoPath = sc.getConf.get("spark.app.IotBSInfoPath", "/user/iot/data/basic/IotBSInfo/data/")
    val bsInfoTable = "IOTBSInfoTable"
    sqlContext.read.format("orc").load(iotBSInfoPath).registerTempTable(bsInfoTable)

    val userDataPath = userPath + "/d=" + userDataTime
    val userDF = sqlContext.read.format("orc").load(userDataPath).filter("is3g='Y' and is4g='N'")
      .selectExpr("mdn", "custid", "ind_type", "ind_det_type", "prodtype", "beloprov", "belocity")
    val tmpUserTable = "spark_tmpUser"
    userDF.registerTempTable(tmpUserTable)
    val userTable = "spark_User"
    sqlContext.sql(
      s"""
         |cache table ${userTable}
         |as
         |select mdn, custid, ind_type, ind_det_type, prodtype, beloprov, belocity
         |from ${tmpUserTable}
       """.stripMargin)

    // 关联基本信息
    val mdnDF = sqlContext.sql(
      s"""
         |select  c.mdn, c.siteid, c.acce_province as provid, c.acce_region as lanid, c.bsid,
         |        c.pdsn_address as PDSNIP,
         |        u.ind_type as industry_level1, u.ind_det_type as industry_level2, u.prodtype as industry_form,
         |        u.beloprov as own_provid, u.belocity as own_lanid, c.tac as TerminalModel,
         |        c.upflow, c.downflow, c.home_agent as HAIP, c.service_option
         |from ${cdrTempTable} c
         |inner join ${userTable} u on(c.mdn = u.mdn)
         |left join ${bsInfoTable} b on(c.siteid = b.enbid)
       """.stripMargin)

    val cdrMdnTable = "spark_cdrmdn"
    mdnDF.registerTempTable(cdrMdnTable)

    val resultDF = sqlContext.sql(
      s"""
         |select mdn, siteid, provid, lanid, bsid, PDSNIP, '-1' as apn,
         |        industry_level1, industry_level2, industry_form, own_provid, own_lanid, TerminalModel,
         |        '-1' as busi, upflow, downflow, sessions, '-1' as duration, HAIP, service_option
         |from(
         |    select mdn, siteid, provid, lanid, bsid, PDSNIP,
         |        industry_level1, industry_level2, industry_form, own_provid, own_lanid, TerminalModel,
         |        sum(upflow) as upflow, sum(downflow) as downflow,
         |        count(mdn) as sessions, HAIP, service_option
         |    from ${cdrMdnTable}
         |    group by mdn, siteid, provid, lanid, bsid, PDSNIP,
         |        industry_level1, industry_level2, industry_form, own_provid, own_lanid, TerminalModel,
         |        HAIP, service_option
         |) t
       """.stripMargin)

    resultDF.repartition(10).write.mode(SaveMode.Overwrite).format("orc").save(outputPath + "dayid=" + dayid)

    sqlContext.sql("use " + ConfigProperties.IOT_HIVE_DATABASE)
    val partitonTable = "iot_stat_cdr_haccg_day"
    val sql = s"alter table $partitonTable add IF NOT EXISTS partition(dayid='$dayid')"
    sqlContext.sql(sql)

  }

}

