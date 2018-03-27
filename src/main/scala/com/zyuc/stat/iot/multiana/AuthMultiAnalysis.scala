package com.zyuc.stat.iot.multiana

import com.zyuc.stat.properties.ConfigProperties
import com.zyuc.stat.utils.DateUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.{Logging, SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

import org.apache.spark.sql.functions._

/**
  * Created by zhoucw on 17-8-3.
  */
object AuthMultiAnalysis extends Logging {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    //.setMaster("local[4]").setAppName("fd")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    sqlContext.sql("use " + ConfigProperties.IOT_HIVE_DATABASE)

    val beginMinu = "201707311220"
    val interval = 80 // 时间间隔 -- 分钟
    val endMinu = DateUtils.timeCalcWithFormatConvertSafe(beginMinu, "yyyyMMddHHmm", interval * 60, "yyyyMMddHHmm")
    val beginD = beginMinu.substring(2, 8)
    val beginH = beginMinu.substring(8, 10)
    val endD = endMinu.substring(2, 8)
    val endH = endMinu.substring(8, 10)
    val beginStr = DateUtils.timeCalcWithFormatConvertSafe(beginMinu, "yyyyMMddHHmm", 0, "yyyyMMddHHmmss")
    val endStr = DateUtils.timeCalcWithFormatConvertSafe(endMinu, "yyyyMMddHHmm", 0, "yyyyMMddHHmmss")

    val inputPath = "/hadoop/IOT/ANALY_PLATFORM/AuthLog/secondETLData/3g/data"

    val sourceDF = sqlContext.read.format("orc").load(inputPath).filter("d>=" + beginD).filter("h>=" + beginH).
      filter("authtime>=" + beginStr).filter("d<=" + endD).filter("h<=" + endH).filter("authtime<" + endStr)

    var filterDF: DataFrame = null
    if (endD > beginD) {
      val intervalDayNums = (DateUtils.timeInterval(beginMinu.substring(0, 8), endMinu.substring(0, 8), "yyyyMMdd")) / (24 * 3600)
      for (i <- 0 to intervalDayNums.toInt) {
        if (i == 0) {
          filterDF = sourceDF.filter("d=" + beginD).filter("h=" + beginH).filter("authtime>=" + beginStr)
        } else if (i == intervalDayNums) {
          filterDF = filterDF.unionAll(sourceDF.filter("d=" + endD).filter("h=" + endH).filter("authtime<" + endStr))
        } else {
          val curD = DateUtils.timeCalcWithFormatConvertSafe(beginMinu, "yyyyMMdd", i * 24 * 60 * 60, "yyMMdd")
          filterDF = filterDF.unionAll(sourceDF.filter("d=" + curD))
        }
      }
    } else if (endD == beginD && endH > beginH) {
      val intervalHourNums = (DateUtils.timeInterval(beginMinu.substring(0, 10), endMinu.substring(0, 10), "yyyyMMdd")) / 3600
      for (i <- 0 to intervalHourNums.toInt) {
        if (i == 0) {
          filterDF = sourceDF.filter("d=" + beginD).filter("h=" + beginH).filter("authtime>=" + beginStr)
        } else if (i == intervalHourNums) {
          filterDF = filterDF.unionAll(sourceDF.filter("d=" + beginD).filter("h=" + endH).filter("authtime<" + endStr))
        } else {
          val curH = DateUtils.timeCalcWithFormatConvertSafe(beginMinu, "yyyyMMddHH", i * 60 * 60, "HH")
          filterDF = filterDF.unionAll(sourceDF.filter("d=" + beginD).filter("h=" + curH))
        }

        if (i == intervalHourNums) {
          filterDF = filterDF.unionAll(sourceDF.filter("d=" + beginD).filter("h=" + endH).filter("authtime<" + endStr))
        }

      }
    } else if (endD == beginD && beginH == endH) {
      filterDF = sourceDF.filter("d=" + beginD).filter("h=" + beginH).filter("authtime>=" + beginStr).filter("authtime<" + endStr)
    } else {
      logError(s"beginMinu:$beginMinu , interval: $interval, Params invalid, please check.")
      return
    }

    val resultDF = filterDF.groupBy(sourceDF.col("custprovince"), sourceDF.col("vpdncompanycode"),sourceDF.col("result"),
      sourceDF.col("auth_result").as("errorcode")).agg(count(lit(1)).alias("requestcnt"),
      sum(when (col("result")==="failed",1).otherwise(0)).alias("reqfailedcnt"),
      countDistinct(sourceDF.col("mdn")).as("mdncnt")).withColumn("starttime", lit(beginMinu)).
      withColumn("endtime", lit(endMinu)).withColumn("authlogtype", lit("3g"))


  }


}
