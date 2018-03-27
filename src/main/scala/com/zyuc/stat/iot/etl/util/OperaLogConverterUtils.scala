package com.zyuc.stat.iot.etl.util

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, regexp_replace, lit, when}


/**
  * Created by zhoucw on 17-7-23.
  */
object OperaLogConverterUtils {
  val Oper_PCRF_PLATFORM:String = "PCRF"
  val Oper_HSS_PLATFORM:String = "HSS"
  val Oper_HLR_PLATFORM:String = "HLR"

  val Oper_OPEN = "open"
  val Oper_CLOSE = "close"

  def parse(operDF: DataFrame, userDF:DataFrame, platformName:String) = {

    operDF.filter(operDF.col("OPER_RESULT")==="成功").join(userDF, operDF.col("mdn")===userDF.col("mdn"), "left").select(operDF.col("detailinfo"),operDF.col("errorinfo"),
      operDF.col("imsicdma"),operDF.col("imsilte"),operDF.col("mdn"),operDF.col("netype"),
      operDF.col("node"),operDF.col("operclass"),operDF.col("opertype"),operDF.col("oper_result"),
      operDF.col("opertime"), userDF.col("vpdncompanycode"), userDF.col("custprovince")).
      withColumn("opername", when(operDF.col("opertype")==="开户", Oper_OPEN).when(operDF.col("opertype")==="销户", Oper_CLOSE).otherwise(operDF.col("opertype"))).
      withColumn("logType", lit(platformName)).
      withColumn("d", regexp_replace(col("opertime"), "[: -]", "").substr(3,6))
  }
}
