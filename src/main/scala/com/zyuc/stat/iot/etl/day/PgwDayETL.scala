package com.zyuc.stat.iot.etl.day

import com.zyuc.stat.properties.ConfigProperties
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by liuzk on 18-5-29.
  */
object PgwDayETL {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()//.setMaster("local[2]").setAppName("name_20180518")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)

    val appName = sc.getConf.get("spark.app.name")
    val inputPath = sc.getConf.get("spark.app.inputPath", "/user/iot_ete/data/cdr/transform/pgw/data")
    val outputPath = sc.getConf.get("spark.app.outputPath","/user/iot_ete/data/cdr/summ_d/pgw/")
    val userPath = sc.getConf.get("spark.app.userPath", "/user/iot/data/baseuser/data/")
    val userDataTime = sc.getConf.get("spark.app.userDataTime", "20180510")

    val dataTime = appName.substring(appName.lastIndexOf("_") + 1)
    val d = dataTime.substring(2, 8)
    val dayid = dataTime.substring(0, 8)
    val partitionPath = s"/d=$d/h=*/m5=*"

    val cdrTempTable = "pgwTempTable"
    sqlContext.read.format("orc").load(inputPath + partitionPath)
      .selectExpr("mdn","enbid","prov","city","t806","servingnodeaddress","accesspointnameni",
        "l_datavolumefbcuplink as upflow","l_datavolumefbcdownlink as downflow",
        "substr(servedimeisv,1,8) as tac","rattype","duration","p_gwaddress", "l_timeoffirstusage as times")//次数
      .registerTempTable(cdrTempTable)


    // 基站
    val iotBSInfoPath = sc.getConf.get("spark.app.IotBSInfoPath", "/user/iot/data/basic/IotBSInfo/data/")
    val bsInfoTable = "IOTBSInfoTable"
    sqlContext.read.format("orc").load(iotBSInfoPath).registerTempTable(bsInfoTable)


    val userDataPath = userPath + "/d=" + userDataTime
    val userDF = sqlContext.read.format("orc").load(userDataPath).filter("is4g='Y'")
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

//    // 关联基本信息
//    val mdnDF = sqlContext.sql(
//      s"""
//         |select  u.custid, c.mdn, c.enbid, c.prov as provid, nvl(b.cityname,'-') as lanid,
//         |        zhLabel, userLabel, vendorId, vndorName,  c.t806 as eci,
//         |        c.servingnodeaddress as sgwip, c.accesspointnameni as apn,
//         |        u.ind_type as industry_level1, u.ind_det_type as industry_level2, u.prodtype as industry_form,
//         |        u.beloprov as own_provid, u.belocity as own_lanid, c.rattype, c.tac as TerminalModel,
//         |        c.upflow, c.downflow, c.duration, c.p_gwaddress as PGWIP, c.times
//         |from ${cdrTempTable} c
//         |inner join ${userTable} u on(c.mdn = u.mdn)
//         |left join ${bsInfoTable} b on(c.enbid = b.enbid and c.prov=b.provname)
//       """.stripMargin)
//
//    val cdrMdnTable = "spark_cdrmdn"
//    //mdnDF.coalesce(200).write.mode(SaveMode.Overwrite).format("orc").save(outputPath + "tmp")
//    mdnDF.write.mode(SaveMode.Overwrite).format("orc").save(outputPath + "tmp")
//    sqlContext.read.format("orc").load(outputPath + "tmp").registerTempTable(cdrMdnTable)
//
//    val resultDF = sqlContext.sql(
//      s"""
//         |select custid, mdn, enbid, provid, lanid,
//         |       zhLabel, userLabel, vendorId, vndorName, eci, sgwip, apn,
//         |       industry_level1, industry_level2, industry_form, own_provid, own_lanid, rattype, TerminalModel,
//         |       '-1' as busi, upflow, downflow, sessions, duration, times, PGWIP
//         |from(
//         |    select custid, mdn, enbid, provid, lanid,
//         |           zhLabel, userLabel, vendorId, vndorName, eci, sgwip, apn,
//         |        industry_level1, industry_level2, industry_form, own_provid, own_lanid, rattype, TerminalModel,
//         |        sum(upflow) as upflow, sum(downflow) as downflow,
//         |        count(mdn) as sessions, sum(duration) as duration, count(distinct times) as times, PGWIP
//         |    from ${cdrMdnTable}
//         |    group by custid, mdn, enbid, provid, lanid,
//         |             zhLabel, userLabel, vendorId, vndorName, eci, sgwip, apn,
//         |        industry_level1, industry_level2, industry_form, own_provid, own_lanid, rattype, TerminalModel,
//         |        PGWIP
//         |) t
//       """.stripMargin)
//
//    resultDF.write.mode(SaveMode.Overwrite).format("orc").save(outputPath + "dayid=" + dayid)
//    //resultDF.coalesce(100).write.mode(SaveMode.Overwrite).format("orc").save(outputPath + "dayid=" + dayid)

      // 关联基本信息
    val mdnDF = sqlContext.sql(
      s"""
         |select  c.mdn, c.enbid, c.prov as provid, nvl(b.cityname,'-') as lanid,
         |        c.t806 as eci,
         |        c.servingnodeaddress as sgwip, c.accesspointnameni as apn,
         |        c.rattype, c.tac as TerminalModel,
         |        c.upflow, c.downflow, c.duration, c.p_gwaddress as PGWIP, c.times
         |from ${cdrTempTable} c
         |left join ${bsInfoTable} b on(c.enbid = b.enbid and c.prov=b.provname)
           """.stripMargin)
    //zhLabel, userLabel, vendorId, vndorName,  c.t806 as eci,
    val cdrMdnTable = "spark_cdrmdn"
    //mdnDF.coalesce(200).write.mode(SaveMode.Overwrite).format("orc").save(outputPath + "tmp")
    mdnDF.write.mode(SaveMode.Overwrite).format("orc").save(outputPath + "tmp")
    sqlContext.read.format("orc").load(outputPath + "tmp").registerTempTable(cdrMdnTable)

    val joinedTable = "joinedTable"
    sqlContext.sql(
      s"""
         |select mdn, enbid, provid, lanid,
         |       eci, sgwip, apn,
         |       rattype, TerminalModel,
         |       '-1' as busi, upflow, downflow, sessions, duration, times, PGWIP
         |from(
         |    select mdn, enbid, provid, lanid,
         |           eci, sgwip, apn,
         |        rattype, TerminalModel,
         |        sum(upflow) as upflow, sum(downflow) as downflow,
         |        count(mdn) as sessions, sum(duration) as duration, count(distinct times) as times, PGWIP
         |    from ${cdrMdnTable}
         |    group by mdn, enbid, provid, lanid,
         |             eci, sgwip, apn,
         |        rattype, TerminalModel,
         |        PGWIP
         |) t
       """.stripMargin).registerTempTable(joinedTable)

    val resultDF = sqlContext.sql(
      s"""
         |select  custid, j.mdn, enbid, provid, lanid,
         |        eci, sgwip, apn,
         |        u.ind_type as industry_level1, u.ind_det_type as industry_level2, u.prodtype as industry_form,
         |        u.beloprov as own_provid, u.belocity as own_lanid, rattype, TerminalModel,
         |        '-1' as busi, upflow, downflow, sessions, duration, times, PGWIP
         |from ${joinedTable} j
         |inner join
         |${userTable} u on(j.mdn = u.mdn)
       """.stripMargin)
      //industry_level1, industry_level2, industry_form, own_provid, own_lanid,

      resultDF.write.mode(SaveMode.Overwrite).format("orc").save(outputPath + "dayid=" + dayid)
      //resultDF.coalesce(100).write.mode(SaveMode.Overwrite).format("orc").save(outputPath + "dayid=" + dayid)

//    sqlContext.sql("use " + ConfigProperties.IOT_HIVE_DATABASE)
//    val partitonTable = "iot_stat_cdr_pgw_day"
//    val sql = s"alter table $partitonTable add IF NOT EXISTS partition(dayid='$dayid')"
//    sqlContext.sql(sql)

  }

}
