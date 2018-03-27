package com.zyuc.stat.iot.cdr

import com.zyuc.stat.iot.service.CDRDataService
import com.zyuc.stat.properties.ConfigProperties
import com.zyuc.stat.utils.DateUtils
import org.apache.spark.{Logging, SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.functions._

/**
  * Created by zhoucw on 17-8-7.
  */
object CDRAnalysis extends Logging{


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
    val pgwTable = sc.getConf.get("spark.app.table.pgwTable")     // iot_cdr_data_pgw
    val pdsnTable = sc.getConf.get("spark.app.table.pdsnTable")   // iot_cdr_data_pdsn
    // val pgwLocation = sc.getConf.get("spark.app.pgwLocation")    //  "/hadoop/IOT/data/cdr/pgw/output/data/"
    // val pdsnLocation = sc.getConf.get("spark.app.pdsnLocation")  //  "/hadoop/IOT/data/cdr/pdsn/output/data/"

    val tmpUserTable = appName + "_usertable_" +  starttimeid
    val userSQL =
      s"""
         |select mdn, imsicdma, custprovince, case when length(vpdncompanycode)=0 then 'N999999999' else vpdncompanycode end  as vpdncompanycode
         |from ${userTable} where d=${userTablePartitionID}
       """.stripMargin
    logInfo("userSQL: " + userSQL)
    sqlContext.sql(userSQL).registerTempTable(tmpUserTable)


    // pdsn flow
    // val pdsnSrcDF = sqlContext.read.format("orc").load(pdsnLocation)

    val pdsnSrcDF = sqlContext.sql(
      s"""select mdn, nvl(originating,0) as upflow, nvl(termination,0) as downflow, 'pdsn' as type
         |from $pdsnTable
         |where d='${partitionD}' and h='${partitionH}' and m5='${partitionM5}'
       """.stripMargin)

    val pgwSrcDF = sqlContext.sql(
      s"""select mdn, nvl(l_datavolumefbcuplink,0) as upflow, nvl(l_datavolumefbcdownlink,0) as downflow, 'pgw' as type
         |from $pgwTable
         |where d='${partitionD}' and h='${partitionH}' and m5='${partitionM5}'
       """.stripMargin)

    val tmpPdsnTable = "tmpPdsnTable_" + starttimeid
    val tmpPgwTable = "tmpPgwTable" + starttimeid

    pdsnSrcDF.registerTempTable(tmpPdsnTable)
    pgwSrcDF.registerTempTable(tmpPgwTable)

    val aggSQL =
      s"""
         |select u.vpdncompanycode, t.type, nvl(sum(t.upflow),0) as upflow, nvl(sum(t.downflow),0) as downflow
         |from
         |(
         |    select mdn, type, upflow, downflow from ${tmpPdsnTable}
         |    union all
         |    select mdn, type, upflow, downflow from ${tmpPgwTable}
         |) t, ${tmpUserTable} u
         |where t.mdn=u.mdn
         |group by u.vpdncompanycode, t.type
       """.stripMargin

    val cdrDF = sqlContext.sql(aggSQL).coalesce(1)
    CDRDataService.saveRddData(cdrDF, starttimeid)


    import sqlContext.implicits._
    val hbaseDF = CDRDataService.registerCdrRDD(sc, starttimeid).toDF()
    CDRDataService.SaveRddToAlarm(sc, sqlContext, hbaseDF, starttimeid)


    sc.stop()

  }

}
