package com.zyuc.stat.nbiot.analysis.day.AbnormalCard

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by liuzk on 18-1-18.
  */
object NbFluxWeekAnalysis {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()//.setMaster("local[2]").setAppName("name_20180118")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    val appName = sc.getConf.get("spark.app.name")
    val inputPath = sc.getConf.get("spark.app.inputPath", "/user/iot/data/cdr/abnormalCard/summ_d/nb/")
    val outputPath = sc.getConf.get("spark.app.outputPath","/user/iot/data/cdr/abnormalCard/summ_w/nb/")

    val onedayago = sc.getConf.get("spark.app.onedayago","190120")
    val twodayago = sc.getConf.get("spark.app.twodayago","190119")
    val threedayago = sc.getConf.get("spark.app.threedayago","190118")
    val fourdayago = sc.getConf.get("spark.app.fourdayago","190117")
    val fivedayago = sc.getConf.get("spark.app.fivedayago","190116")
    val sixdayago = sc.getConf.get("spark.app.sixdayago","190115")
    val sevendayago = sc.getConf.get("spark.app.sevendayago","190114")

    val fileSystem = FileSystem.get(sc.hadoopConfiguration)
    val dataTime = appName.substring(appName.lastIndexOf("_") + 1)
    val d = onedayago
    //val d = dataTime.substring(2, 8)

    //将7天 日基表注册成表
    val baseTable = "baseTable"
    sqlContext.read.format("orc").load(inputPath + onedayago + "/baseFlux",
      inputPath + twodayago + "/baseFlux", inputPath + threedayago + "/baseFlux",
      inputPath + fourdayago + "/baseFlux", inputPath + fivedayago + "/baseFlux",
      inputPath + sixdayago + "/baseFlux", inputPath + sevendayago + "/baseFlux").registerTempTable(baseTable)

    val baseFlux = sqlContext.sql(
                          s"""
                             |select custid, avg(avgUpflow) as avgUpflow, avg(avgDownflow) as avgDownflow,
                             |        avg(totalFlow) as avgTotalFlow, sum(cnt) as cnt
                             |from ${baseTable}
                             |group by custid
                           """.stripMargin)

    val baseFluxDF = baseFlux.selectExpr("custid", "'NB' as netType", "'周' as anaCycle", s"'${d}' as summ_cycle",
      "avgUpflow", "avgDownflow", "avgTotalFlow",
      "'-1' as upPacket", "'-1' as downPacket", "'-1' as totalPacket", "cnt")
    // 周基表保存到hdfs
    baseFluxDF.filter("custid is not null and custid!=''").coalesce(20)
      .write.format("orc").mode(SaveMode.Overwrite).save(outputPath + d + "/baseFlux")



    //将7天 异常 日基表注册成表
    val abnormalFluxTable = "abnormalFluxTable"
    sqlContext.read.format("orc").load(outputPath + onedayago + "/abnormalFlux",
      outputPath + twodayago + "/abnormalFlux", outputPath + threedayago + "/abnormalFlux",
      outputPath + fourdayago + "/abnormalFlux", outputPath + fivedayago + "/abnormalFlux",
      outputPath + sixdayago + "/abnormalFlux", outputPath + sevendayago + "/abnormalFlux").registerTempTable(abnormalFluxTable)

    val abnormalFlux = sqlContext.sql(
                        s"""
                           |select custid, mdn, avg(avgUpflow) as avgUpflow,
                           |        avg(avgDownflow) as avgDownflow, avg(avgFlow) as avgFlow
                           |from ${abnormalFluxTable}
                           |group by custid, mdn
                         """.stripMargin)

    val abnormalFluxDF = abnormalFlux.selectExpr("custid", s"'${d}' as summ_cycle", "mdn",
                                                  "avgUpflow", "avgDownflow", "avgFlow",
                                                  "'-1' as upPacket", "'-1' as downPacket", "'-1' as totalPacket")
    // 周异常基表保存到hdfs
    abnormalFluxDF.coalesce(20).write.format("orc").mode(SaveMode.Overwrite).save(outputPath + d + "/abnormalFlux")


  }
}
