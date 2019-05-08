package com.zyuc.stat.iot.etl.secondaryTotal.ServiceOption

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by liuzk on 19-4-15.
  *
  * 14135 使用3G业务用户清单月统计         epc-log-nm-30
  */
object PdsnMonthTotal {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()//.setMaster("local[2]").setAppName("name_20180601")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)

    val appName = sc.getConf.get("spark.app.name")
    val inputPath_3G = sc.getConf.get("spark.app.inputPath_3G", "/user/iot_ete/data/cdr/summ_m/pdsn/")
    val inputPath_4G = sc.getConf.get("spark.app.inputPath_4G", "/user/iot_ete/data/cdr/summ_m/pgw/")
    val outputPath = sc.getConf.get("spark.app.outputPath","/user/iot_ete/data/cdr/summ_d/pdsn_ServiceOption/")
    val userPath = sc.getConf.get("spark.app.userPath", "/user/iot/data/baseuser/data/")
    val userDataTime = sc.getConf.get("spark.app.userDataTime", "20180510")

    val dataTime = appName.substring(appName.lastIndexOf("_") + 1)
    val monthid = dataTime.substring(0, 6)

    // CRM数据
    val userDataPath = userPath + "/d=" + userDataTime
    val userDF = sqlContext.read.format("orc").load(userDataPath)
      .selectExpr("mdn", "custid")
    val tmpUserTable = "spark_tmpUser"
    userDF.registerTempTable(tmpUserTable)
    val userTable = "spark_User"
    sqlContext.sql(
      s"""
         |cache table ${userTable}
         |as
         |select mdn, custid
         |from ${tmpUserTable}
       """.stripMargin)



    val df3G = sqlContext.read.format("orc").load(inputPath_3G + "monthid=" + monthid)
      .filter("service_option='59'").selectExpr("mdn","upflow","downflow")

    val df4G = sqlContext.read.format("orc").load(inputPath_4G + "monthid=" + monthid)
      .filter("rattype='102'").selectExpr("mdn","upflow","downflow")

    val tempTable_Union = "tempTable_Union"
    df3G.unionAll(df4G).registerTempTable(tempTable_Union)

    val dfUnion = sqlContext.sql(
            s"""
               |select t.mdn, u.custid, sum(upflow) as UpFlow, sum(downflow) as Downflow, sum(upflow+downflow) as TotalFlow
               |from $tempTable_Union t
               |left join $userTable u on(t.mdn=u.mdn)
               |group by t.mdn, u.custid
             """.stripMargin)
    dfUnion.coalesce(20).write.mode(SaveMode.Overwrite).format("orc").save(outputPath + "monthid=" + monthid)



  }

}

