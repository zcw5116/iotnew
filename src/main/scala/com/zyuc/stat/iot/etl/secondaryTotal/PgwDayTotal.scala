package com.zyuc.stat.iot.etl.secondaryTotal

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by liuzk on 18-8-1.
  */
object PgwDayTotal {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()//.setMaster("local[2]").setAppName("name_20180805")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)

    val appName = sc.getConf.get("spark.app.name")
    val dayid = appName.substring(appName.lastIndexOf("_") + 1)
    val inputPath = sc.getConf.get("spark.app.inputPath", "/user/iot_ete/data/cdr/summ_d/pgw/")
    val outputPath = sc.getConf.get("spark.app.outputPath", "/user/iot_ete/data/cdr/summ_d/pgwTotal/")
    sqlContext.read.format("orc").load(inputPath + "dayid=" + dayid).registerTempTable("dayTable")
    sqlContext.sql(
      s"""
         |select provid,lanid,eci,apn,busi,industry_form,
         |        sum(sessions) as sessions, sum(upflow) as upflow,
         |        sum(downflow) as downflow,sum(upflow+downflow) as totalFlows
         |from dayTable
         |group by provid,lanid,eci,apn,busi,industry_form
       """.stripMargin).coalesce(1).write.format("orc").mode(SaveMode.Overwrite).save(outputPath + "dayid=" + dayid)

  }

}
