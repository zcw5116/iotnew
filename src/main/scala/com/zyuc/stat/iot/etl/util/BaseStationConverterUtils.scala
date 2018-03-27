package com.zyuc.stat.iot.etl.util

import org.apache.spark.Logging
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.Row


/**
  * Created by zhoucw on 17-7-23.
  */
object BaseStationConverterUtils extends Logging{

  val struct = StructType(Array(
    StructField("enbid", StringType),
    StructField("provId", StringType),
    StructField("provName", StringType),
    StructField("cityId", StringType),
    StructField("cityName", StringType),
    StructField("zhLabel", StringType),
    StructField("userLabel", StringType),
    StructField("vendorId", StringType),
    StructField("vndorName", StringType)
  ))



    def parseLine(line:String) :Row = {
      try {
        val p = line.replaceAll("\"","").split(",", 10)
        val enbid = p(0)
        val provId = if(null == p(1)) "" else p(1)
        val provName = if(null == p(2)) "" else p(2)
        val cityId = if(null == p(3)) "" else p(3)   // regionId, 应该是oracle表数据字段对应错误
        val cityName = if(null == p(4)) "" else p(4) // regionName, 应该是oracle表数据字段对应错误
        val zhLabel = if(null == p(5)) "" else p(5)
        val userLabel = if(null == p(6)) "" else p(6)
        val vendorId = if(null == p(7)) "" else p(7)
        val vndorName = if(null == p(8)) "" else p(8)

        Row(enbid, provId, provName, cityId, cityName, zhLabel, userLabel, vendorId, vndorName)
      }catch {
        case e:Exception => {
          //logError("ParseError log[" + line + "] msg[" + e.getMessage + "]")
          Row("0")
        }
      }
    }
}
