package com.zyuc.stat.iot.etl.secondaryTotal.pdsnMonthAnalysis

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by liuzk on 19-10-14
  *
  * 物联网各省3G接入漫游信息统计
  */
object PdsnMonthProvAnalysis {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()//.setMaster("local[2]").setAppName("name_20191014")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)

    val appName = sc.getConf.get("spark.app.name")
    val inputPath = sc.getConf.get("spark.app.inputPath", "/user/iot_ete/data/cdr/summ_d/pdsn/")
    val outputPath = sc.getConf.get("spark.app.outputPath", "/user/iot/tmp/PdsnAnalysis/PdsnMonthProvAnalysis/")
    val monthid = appName.substring(appName.lastIndexOf("_") + 1)

    val df = sqlContext.read.format("orc").load(inputPath + s"dayid=${monthid}*")
      //.filter("provid='山东'")
      .selectExpr("provid","lanid","bsid", "sid", "mdn","own_provid","industry_level2","custname","upflow","downflow")

    df.coalesce(200).write.mode(SaveMode.Overwrite).format("orc").save(outputPath + "tmp")


    val table_cdr = "table_cdr"
    sqlContext.read.format("orc").load(outputPath + "tmp").registerTempTable(table_cdr)



    val resultDF = sqlContext.sql(
      s"""
         |select provid, lanid, bsid, sid, mdn, own_provid, industry_level2, custname,
         |       sum(upflow) upflow, sum(downflow) downflow, sum(upflow+downflow) totalflow
         |from ${table_cdr}
         |group by provid, lanid, bsid, sid, mdn, own_provid, industry_level2, custname
       """.stripMargin)

    var resultCsv = Map("header" -> "true", "delimiter" -> ",", "path" -> s"${outputPath}data")

    resultDF.repartition(1).write.format("com.databricks.spark.csv").mode(SaveMode.Overwrite).options(resultCsv).save()



  }

}
