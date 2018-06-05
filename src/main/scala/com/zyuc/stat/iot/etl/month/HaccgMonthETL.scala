package com.zyuc.stat.iot.etl.month

import com.zyuc.stat.properties.ConfigProperties
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by liuzk on 18-5-29.
  */
object HaccgMonthETL {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()//.setMaster("local[2]").setAppName("name_201806")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)

    val appName = sc.getConf.get("spark.app.name")
    val inputPath = sc.getConf.get("spark.app.inputPath", "/user/iot/data/cdr/summ_d/haccg")
    val outputPath = sc.getConf.get("spark.app.outputPath","/user/iot/data/cdr/summ_m/haccg/")

    val dataTime = appName.substring(appName.lastIndexOf("_") + 1)
    val monthid = dataTime.substring(0, 6)
    val partitionPath = s"/dayid=$monthid*"

    val cdrTempTable = "haccgMonthTable"
    sqlContext.read.format("orc").load(inputPath + partitionPath).registerTempTable(cdrTempTable)


    val resultDF = sqlContext.sql(
      s"""
         |select mdn, provid, lanid, bsid, PDSNIP, '-1' as apn,
         |        industry_level1, industry_level2, industry_form, own_provid, own_lanid, net, TerminalModel,
         |        '-1' as busi, upflow, downflow, sessions, '-1' as duration, HAIP
         |from(
         |    select mdn, provid, lanid, bsid, PDSNIP,
         |        industry_level1, industry_level2, industry_form, own_provid, own_lanid, net, TerminalModel,
         |        sum(upflow) as upflow, sum(downflow) as downflow,
         |        count(distinct mdn) as sessions, HAIP
         |    from ${cdrTempTable}
         |    group by mdn, provid, lanid, bsid, PDSNIP,
         |        industry_level1, industry_level2, industry_form, own_provid, own_lanid, net, TerminalModel,
         |        HAIP
         |) t
       """.stripMargin)

    resultDF.coalesce(10).write.mode(SaveMode.Overwrite).format("orc").save(outputPath + "monthid=" + monthid)

    sqlContext.sql("use " + ConfigProperties.IOT_HIVE_DATABASE)
    val partitonTable = "iot_stat_cdr_haccg_month"
    val sql = s"alter table $partitonTable add IF NOT EXISTS partition(monthid='$monthid')"
    sqlContext.sql(sql)

  }

}



