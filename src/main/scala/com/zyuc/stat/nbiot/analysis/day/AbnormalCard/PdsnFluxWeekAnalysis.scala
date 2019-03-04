package com.zyuc.stat.nbiot.analysis.day.AbnormalCard

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by liuzk on 18-1-18.
  */
object PdsnFluxWeekAnalysis {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()//.setMaster("local[2]").setAppName("name_20180118")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    val appName = sc.getConf.get("spark.app.name")
    val inputPath = sc.getConf.get("spark.app.inputPath", "/user/iot_ete/data/cdr/summ_d/pdsn/")
    val outputPath = sc.getConf.get("spark.app.outputPath","/user/iot/data/cdr/abnormalCard/summ_w/pdsn")

    val userPath = sc.getConf.get("spark.app.userPath", "/user/iot/data/baseuser/data/")
    val userDataTime = sc.getConf.get("spark.app.userDataTime", "20180510")

    val onedayago = sc.getConf.get("spark.app.onedayago","20190120")
    val twodayago = sc.getConf.get("spark.app.twodayago","20190119")
    val threedayago = sc.getConf.get("spark.app.threedayago","20190118")
    val fourdayago = sc.getConf.get("spark.app.fourdayago","20190117")
    val fivedayago = sc.getConf.get("spark.app.fivedayago","2190116")
    val sixdayago = sc.getConf.get("spark.app.sixdayago","20190115")
    val sevendayago = sc.getConf.get("spark.app.sevendayago","20190114")

    val fileSystem = FileSystem.get(sc.hadoopConfiguration)
    val dataTime = appName.substring(appName.lastIndexOf("_") + 1)
    val d = dataTime.substring(2, 8)

    val cdrTempTable = "CDRTempTable"
    sqlContext.read.format("orc").load(inputPath + "dayid=" + onedayago,
      inputPath + "dayid=" + twodayago, inputPath + "dayid=" + threedayago,
      inputPath + "dayid=" + fourdayago, inputPath + "dayid=" + fivedayago,
      inputPath + "dayid=" + sixdayago, inputPath + "dayid=" + sevendayago)
      .selectExpr("mdn","upflow","downflow")
      .registerTempTable(cdrTempTable)

    val userDataPath = userPath + "/d=" + userDataTime
    val userDF = sqlContext.read.format("orc").load(userDataPath).filter("is3g='Y' or is4g='Y'")
      .filter("beloprov='江苏'").selectExpr("mdn","custid")
    val tmpUserTable = "spark_tmpUser"
    userDF.registerTempTable(tmpUserTable)

    val userTable = "spark_User"
    sqlContext.sql(
      s"""
         |cache table ${userTable}
         |as
         |select mdn, custid
         |from ${tmpUserTable}
       """.stripMargin)


    //------------先将两表合并完了，保存到hdfs在分析
    sqlContext.sql(
      s"""
         |select custid, c.mdn, upflow, downflow
         |from ${cdrTempTable} c
         |left join ${userTable} u
         |on c.mdn=u.mdn
       """.stripMargin).coalesce(70).write.format("orc").mode(SaveMode.Overwrite).save(outputPath + "/" + d + "/base")

    val baseTable = "baseTable"
    sqlContext.read.format("orc").load(outputPath + "/" + d + "/base")
      .filter("custid is not null and custid!=''").registerTempTable(baseTable)


    val baseFlux = sqlContext.sql(
      s"""
         |select custid, avg(upflow) as avgUpflow, avg(downflow) as avgDownflow,
         |       avg(totalFlow) as avgTotalFlow, count(distinct mdn) as cnt
         |from
         |(
         |select custid, mdn, upflow, downflow, (upflow+downflow) as totalFlow,
         |       row_number() over(partition by custid order by (upflow+downflow) asc ) rank,
         |       count(1) over(partition by custid) maxRank
         |from ${baseTable}
         |) a
         |where rank*100/maxRank > 10 and rank*100/maxRank < 90
         |group by custid
        """.stripMargin)

    val baseFluxDF = baseFlux.selectExpr("custid", "'3G' as netType", "'周' as anaCycle", s"'${d}' as summ_cycle",
      "avgUpflow", "avgDownflow", "avgTotalFlow",
      "'-1' as upPacket", "'-1' as downPacket", "'-1' as totalPacket", "cnt")
    //基表保存到hdfs
    baseFluxDF.filter("custid is not null and custid!=''").coalesce(70)
      .write.format("orc").mode(SaveMode.Overwrite).save(outputPath + "/" + d + "/baseFlux")


    //基表部分字段注册成临时表
    val baseFluxTable = "baseFluxTable"
    sqlContext.read.format("orc").load(outputPath + "/" + d + "/baseFlux").filter("cnt>100")
      .selectExpr("custid", "avgTotalFlow").registerTempTable(baseFluxTable)

    val abnormalFlux = sqlContext.sql(
      s"""
         |select b.custid, mdn, avg(upflow) as avgUpflow, avg(downflow) as avgDownflow,
         |       avg(upflow+downflow) as avgFlow, avgTotalFlow
         |from ${baseTable} u
         |left join ${baseFluxTable} b on u.custid=b.custid
         |group by b.custid, mdn, avgTotalFlow
        """.stripMargin).filter("avgFlow*100/avgTotalFlow<50 or avgFlow*100/avgTotalFlow>150")

    val abnormalFluxDF = abnormalFlux.selectExpr("custid", s"'${d}' as summ_cycle", "mdn",
      "avgUpflow", "avgDownflow", "avgFlow",
      "'-1' as upPacket", "'-1' as downPacket", "'-1' as totalPacket")
    //异常卡流量保存到hdfs
    abnormalFluxDF.coalesce(70).write.format("orc").mode(SaveMode.Overwrite).save(outputPath + "/" + d + "/abnormalFlux")


  }
}
