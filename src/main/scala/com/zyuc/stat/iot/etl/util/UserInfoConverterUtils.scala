package com.zyuc.stat.iot.etl.util


import org.apache.spark.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.collection.mutable

/**
  * Created by zhoucw on 17-7-27.
  */
object UserInfoConverterUtils extends Logging {


  val struct = StructType(Array(
    StructField("mdn", StringType),
    StructField("imsicdma", StringType),
    StructField("imsilte", StringType),
    StructField("iccid", StringType),
    StructField("imei", StringType),

    StructField("companycode", StringType),
    StructField("vpdncompanycode", StringType),
    StructField("apncompanycode", StringType),
    StructField("nettype", StringType),
    StructField("vpdndomain", StringType),

    StructField("isvpdn", StringType),
    StructField("isdirect", StringType),
    StructField("subscribetimeaaa", StringType),
    StructField("subscribetimehlr", StringType),
    StructField("subscribetimehss", StringType),

    StructField("subscribetimepcrf", StringType),
    StructField("firstactivetime", StringType),
    StructField("userstatus", StringType),
    StructField("atrbprovince", StringType),
    StructField("userprovince", StringType),

    StructField("belo_city", StringType),
    StructField("belo_prov", StringType),
    StructField("custstatus", StringType),
    StructField("custtype", StringType),
    StructField("prodtype", StringType),

    StructField("internetType", StringType),
    StructField("vpdnOnly", StringType),
    StructField("isCommon", StringType)
  ))


  def parseLine(line: String) = {

    try {
      val p = line.split("\\|", 26)
      val apncompanycode = p(7)
      val isDirect = if (apncompanycode.startsWith("D")) "1" else "0"
      val vpdnDomain = p(9)
      /*val domains = vpdnDomain.split(",")
      var apn = ""
      domains.foreach(x=>{
        apn = apn + "," + apnMap(x)
      })*/

      val isVPDN = if (p(10) == "1") "1" else "0"

      val internetType = p(24)
      val vpdnOnly = if(internetType.contains("VpdnBlockInternet")) "1" else "0"
      val isCommon = if(vpdnOnly!="1" && isDirect!="1") "1" else "0"


      Row(p(0), p(1), p(2), p(3), p(4), p(5), p(6), p(7), p(8), p(9), isVPDN, isDirect, p(11), p(12), p(13), p(14), p(15), p(16), p(17), p(18), p(19), p(20), p(21), p(22), p(23), internetType, vpdnOnly, isCommon)
    } catch {
      case e: Exception =>
        logError("ParseError log[" + line + "] msg[" + e.getMessage + "]")
        Row("0")
    }
  }

  def main(args: Array[String]): Unit = {


    println("test")
  }
}
