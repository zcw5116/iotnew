package com.zyuc.stat.iot.etl.util

import org.apache.spark.Logging
import org.apache.spark.sql.DataFrame


/**
  * Created by zhoucw on 17-7-23.
  */
object AuthLogConverterUtils extends Logging{
  val LOG_TYPE_3G:String = "3g"
  val LOG_TYPE_4G:String = "4g"
  val LOG_TYPE_VPDN:String = "vpdn"

  def parse(df: DataFrame, authLogType:String) = {
    var newDF:DataFrame = null
    try{
      if(authLogType==LOG_TYPE_3G){
        newDF = df.selectExpr("auth_result", "auth_time", "device", "imsicdma", "imsilte", "mdn",
          "nai_sercode", "nasport", "nasportid", "nasporttype", "pcfip", "srcip",
          "substr(regexp_replace(auth_time,'-',''),3,6) as d", "substr(auth_time,12,2) as h",
          "floor(substr(auth_time,15,2)/5)*5 as m5")
      }
      else if(authLogType==LOG_TYPE_4G){
        newDF = df.selectExpr("auth_result", "auth_time", "device", "imsicdma", "imsilte", "mdn",
          "nai_sercode", "nasport", "nasportid", "nasporttype", "pcfip",
          "substr(regexp_replace(auth_time,'-',''),3,6) as d", "substr(auth_time,12,2) as h",
          "floor(substr(auth_time,15,2)/5)*5 as m5")
      }
      else if(authLogType==LOG_TYPE_VPDN){
        newDF = df.selectExpr("auth_result", "auth_time", "device", "imsicdma", "imsilte", "mdn",
          "nai_sercode", "entname", "lnsip", "pdsnip",
          "substr(regexp_replace(auth_time,'-',''),3,6) as d", "substr(auth_time,12,2) as h",
          "floor(substr(auth_time,15,2)/5)*5 as m5")
      }
    }catch {
      case e:Exception =>{
        e.printStackTrace()
        logError(s"[ $authLogType ] 失败 处理异常 " + e.getMessage)
      }
    }
    newDF
  }
}
