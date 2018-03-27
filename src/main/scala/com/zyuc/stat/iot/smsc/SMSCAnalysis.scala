package com.zyuc.stat.iot.smsc

import com.zyuc.stat.iot.service.{CDRDataService, SMSCDataService}
import com.zyuc.stat.properties.ConfigProperties
import com.zyuc.stat.utils.DateUtils
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{Logging, SparkConf, SparkContext}

object SMSCAnalysis extends Logging{


  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()//.setAppName("AuthLogAnalysisHbase").setMaster("local[4]")

    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)

    sqlContext.sql("use " + ConfigProperties.IOT_HIVE_DATABASE)
    val appName = sc.getConf.get("spark.app.name")
    val starttimeid = sc.getConf.get("spark.app.starttimeid") // 20170801004000
    val partitionD = starttimeid.substring(2, 8)
    val partitionH = starttimeid.substring(8, 10)
    val partitionM5 = starttimeid.substring(10, 12)
    val dayid = starttimeid.substring(0, 8)
    var userTablePartitionID = DateUtils.timeCalcWithFormatConvertSafe(starttimeid.substring(0,8), "yyyyMMdd", -1*24*3600, "yyyyMMdd")
    userTablePartitionID = sc.getConf.get("spark.app.table.userTablePartitionDayID", userTablePartitionID)
    val userTable = sc.getConf.get("spark.app.table.userTable") //"iot_customer_userinfo"
    val smscTable = sc.getConf.get("spark.app.table.smscTable")     // iot_data_smsc

    val tmpUserTable = appName + "_usertable_" +  starttimeid
    val userSQL =
      s"""
         |select mdn, imsicdma, custprovince, case when length(vpdncompanycode)=0 then 'N999999999' else vpdncompanycode end  as vpdncompanycode
         |from ${userTable} where d=${userTablePartitionID}
       """.stripMargin
    logInfo("userSQL: " + userSQL)
    sqlContext.sql(userSQL).registerTempTable(tmpUserTable)
    val countSQL =
      s"""
         |select u.vpdncompanycode, count(t.Called_Number) as calledCount, count(t.Calling_Number) as callingCount
         |from
         |(
         |    select mdn,Called_Number,Calling_Number from ${smscTable}
         |    where d='${partitionD}' and h='${partitionH}' and m5='${partitionM5}'
         |) t, ${tmpUserTable} u
         |where t.mdn=u.mdn
         |group by u.vpdncompanycode
       """.stripMargin

    val smscDF = sqlContext.sql(countSQL).coalesce(1)
    SMSCDataService.saveRddData(smscDF, starttimeid)
    sc.stop()
  }
}
