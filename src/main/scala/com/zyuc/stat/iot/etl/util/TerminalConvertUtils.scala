package com.zyuc.stat.iot.etl.util

import com.zyuc.stat.utils.DateUtils
import org.apache.spark.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
  * Created by zhoucw on 17-7-26.
  */
object TerminalConvertUtils extends Logging{

  val struct = StructType(Array(
    StructField("tac", StringType),
    StructField("marketingname", StringType),
    StructField("manufacturer_applicant", StringType),
    StructField("modelname", StringType),
    StructField("devicetype", StringType),
    StructField("crttime", StringType)
  ))


  def parseLine(line:String) = {

    try {
      val p = line.split("\\|")
      val crttime = DateUtils.getNowTime("yyyy-MM-dd HH:mm:ss")
      Row(p(0), p(1), p(2), p(3), p(4), crttime)
    }catch {
      case e: Exception =>
        logError("ParseError log[" + line + "] msg[" + e.getMessage + "]")
        Row("0")
    }
  }

}
