package com.zyuc.stat.iotNBLiuzk.analysis.day

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by liuzk on 18-5-10.
  */
object NbCdrDayAnalysis {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("name_20180504")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)

    val appName = sc.getConf.get("spark.app.name")
    val inputPath = sc.getConf.get("spark.app.inputPath", "/user/epciot/data/cdr/transform/nb/data")
    val outputPath = sc.getConf.get("spark.app.outputPath","/user/epciot/data/mme/summ_d/nb")
    val userPath = sc.getConf.get("spark.app.userPath", "/user/epciot/data/baseuser/data/")
    val userDataTime = sc.getConf.get("spark.app.userDataTime", "20180510")

    val dataTime = appName.substring(appName.lastIndexOf("_") + 1)
    val d = dataTime.substring(2, 8)
    val dd = dataTime.substring(0, 8)
    val partitionPath = s"/d=$d/h=*/m5=*"
    val dayPath = s"/d=$d"

    val cdrTempTable = "CDRTempTable"
    sqlContext.read.format("orc").load(inputPath + partitionPath)
      .selectExpr("prov","city","t804 as enbid","l_datavolumefbcuplink as upflow","l_datavolumefbcdownlink as downflow",
        "substr(servedimeisv,1,8) as tac")
      .registerTempTable(cdrTempTable)

    import sqlContext.implicits._
    val terminalTable = "IOTTerminalTable"
    sc.textFile("/user/epciot/data/basic/IOTTerminal/iotterminal.csv")
      .filter(!_.contains("This is a Test IMEI")).map(line=>line.split(",\"",5)).map(x=>(x(0),x(4)))
      .toDF("tac", "devtype").registerTempTable(terminalTable)

    // 基站
    val iotBSInfoPath = sc.getConf.get("spark.app.IotBSInfoPath", "/user/epciot/data/basic/IotBSInfo/data/")
    val bsInfoTable = "IOTBSInfoTable"
    sqlContext.read.format("orc").load(iotBSInfoPath).registerTempTable(bsInfoTable)

    val userDataPath = userPath + "/d=" + userDataTime
    val userDF = sqlContext.read.format("orc").load(userDataPath).filter("isnb='1'")
    val tmpUserTable = "spark_tmpUser"
    userDF.registerTempTable(tmpUserTable)
    val userTable = "spark_User"
    sqlContext.sql(
      s"""
         |cache table ${userTable}
         |as
         |select mdn, custid
         |from ${tmpUserTable}
         |where isnb = '1'
       """.stripMargin)

    // 关联基本信息
    val mdnDF = sqlContext.sql(
      s"""
         |select u.custid, c.mdn, c.enbid, b.provname as prov, b.cityname as city,
         |       c.upflow, c.downflow, c.tac
         |from ${cdrTempTable} c
         |inner join ${userTable} u on(c.mdn = u.mdn)
         |left join ${bsInfoTable} b on(c.enbid = b.enbid)
       """.stripMargin)
    val cdrMdnTable = "spark_cdrmdn"
    mdnDF.registerTempTable(cdrMdnTable)

    // 基站的信息
    val bsStatDF = sqlContext.sql(
      s"""
         |select custid, enbid,
         |       sum(upflow) as upflow, sum(downflow) as downflow,
         |       count(distinct mdn) as activeUsers, count(chargingid) as activeSessions
         |from ${cdrMdnTable}
         |group by custid, enbid
       """.stripMargin)

    // 省份
    val provStatDF = sqlContext.sql(
      s"""
         |select custid, prov,
         |       sum(upflow) as upflow, sum(downflow) as downflow,
         |       count(distinct mdn) as activeUsers, count(chargingid) as activeSessions
         |from ${cdrMdnTable}
         |group by custid, prov
       """.stripMargin)

    //地市
    val cityStatDF = sqlContext.sql(
      s"""
         |select custid, city
         |       sum(upflow) as upflow, sum(downflow) as downflow,
         |       count(distinct mdn) as activeUsers, count(chargingid) as activeSessions
         |from ${cdrMdnTable}
         |group by custid, city
       """.stripMargin)

    //tac
    val tacStatDF = sqlContext.sql(
      s"""
         |select custid, tac
         |       count(distinct mdn) as cnt
         |from ${cdrMdnTable}
         |group by custid, tac
       """.stripMargin)



    val sqlDayAll =
      s"""
         |select '${dd}' as summ_cycle, u.custid,
         |c.prov, c.city, c.t804 as enbid, i.devtype as TERMDETAIL,
         |sum(c.l_datavolumefbcuplink) as INFLOW , sum(c.l_datavolumefbcdownlink) as OUTFLOW,
         |(sum(c.l_datavolumefbcuplink) + sum(c.l_datavolumefbcdownlink)) as TOTALFLOW,
         |count(distinct mdn) as ACTIVEUSERS ,count(chargingid) as SESSIONS
         |from
         |on c.tac=i.tac
         |inner join
         |${userTable} u
         |on c.mdn = u.mdn
         |group by u.custid, c.prov, c.city, c.t804, i.devtype
       """.stripMargin

    //注册为临时表 再select -- as --  union on ---??
    // val rowTable = "rowTable"
    val resultDF = sqlContext.sql(sqlDayAll)//.registerTempTable(rowTable)

    val inflowDF4 = resultDF.selectExpr("summ_cycle", "custid", "prov", "city","enbid","TERMDETAIL", "INFLOW as meas_value").
      withColumn("meas_obj", lit("INFLOW"))
    val inflowDF3 = resultDF.selectExpr("summ_cycle", "custid", "prov", "city","enbid","null", "INFLOW as meas_value").
      withColumn("meas_obj", lit("INFLOW"))
    val inflowDF2 = resultDF.selectExpr("summ_cycle", "custid", "prov", "city","null","null", "INFLOW as meas_value").
      withColumn("meas_obj", lit("INFLOW"))
    val inflowDF1 = resultDF.selectExpr("summ_cycle", "custid", "prov", "null","null","null", "INFLOW as meas_value").
      withColumn("meas_obj", lit("INFLOW"))

    val outflowDF4 = resultDF.selectExpr("summ_cycle", "custid", "prov", "city","enbid","TERMDETAIL", "OUTFLOW as meas_value").
      withColumn("meas_obj", lit("OUTFLOW"))
    val outflowDF3 = resultDF.selectExpr("summ_cycle", "custid", "prov", "city","enbid","null", "OUTFLOW as meas_value").
      withColumn("meas_obj", lit("OUTFLOW"))
    val outflowDF2 = resultDF.selectExpr("summ_cycle", "custid", "prov", "city","null","null", "OUTFLOW as meas_value").
      withColumn("meas_obj", lit("OUTFLOW"))
    val outflowDF1 = resultDF.selectExpr("summ_cycle", "custid", "prov", "null","null","null", "OUTFLOW as meas_value").
      withColumn("meas_obj", lit("OUTFLOW"))

    val totalflowDF4 = resultDF.selectExpr("summ_cycle", "custid", "prov", "city","enbid","TERMDETAIL", "TOTALFLOW as meas_value").
      withColumn("meas_obj", lit("TOTALFLOW"))
    val totalflowDF3 = resultDF.selectExpr("summ_cycle", "custid", "prov", "city","enbid","null", "TOTALFLOW as meas_value").
      withColumn("meas_obj", lit("TOTALFLOW"))
    val totalflowDF2 = resultDF.selectExpr("summ_cycle", "custid", "prov", "city","null","null", "TOTALFLOW as meas_value").
      withColumn("meas_obj", lit("TOTALFLOW"))
    val totalflowDF1 = resultDF.selectExpr("summ_cycle", "custid", "prov", "null","null","null", "TOTALFLOW as meas_value").
      withColumn("meas_obj", lit("TOTALFLOW"))

    val activeusersDF4 = resultDF.selectExpr("summ_cycle", "custid", "prov", "city","enbid","TERMDETAIL", "ACTIVEUSERS as meas_value").
      withColumn("meas_obj", lit("ACTIVEUSERS"))
    val activeusersDF3 = resultDF.selectExpr("summ_cycle", "custid", "prov", "city","enbid","null", "ACTIVEUSERS as meas_value").
      withColumn("meas_obj", lit("ACTIVEUSERS"))
    val activeusersDF2 = resultDF.selectExpr("summ_cycle", "custid", "prov", "city","null","null", "ACTIVEUSERS as meas_value").
      withColumn("meas_obj", lit("ACTIVEUSERS"))
    val activeusersDF1 = resultDF.selectExpr("summ_cycle", "custid", "prov", "null","null","null", "ACTIVEUSERS as meas_value").
      withColumn("meas_obj", lit("ACTIVEUSERS"))

    val sessionsDF4 = resultDF.selectExpr("summ_cycle", "custid", "prov", "city","enbid","TERMDETAIL", "SESSIONS as meas_value").
      withColumn("meas_obj", lit("SESSIONS"))
    val sessionsDF3 = resultDF.selectExpr("summ_cycle", "custid", "prov", "city","enbid","null", "SESSIONS as meas_value").
      withColumn("meas_obj", lit("SESSIONS"))
    val sessionsDF2 = resultDF.selectExpr("summ_cycle", "custid", "prov", "city","null","null", "SESSIONS as meas_value").
      withColumn("meas_obj", lit("SESSIONS"))
    val sessionsDF1 = resultDF.selectExpr("summ_cycle", "custid", "prov", "null","null","null", "SESSIONS as meas_value").
      withColumn("meas_obj", lit("SESSIONS"))

    // 将结果写入到 hdfs
    val outputResult = outputPath + dayPath
    inflowDF4.unionAll(inflowDF3).unionAll(inflowDF2).unionAll(inflowDF1)
      .unionAll(outflowDF4).unionAll(outflowDF3).unionAll(outflowDF2).unionAll(outflowDF1)
      .unionAll(totalflowDF4).unionAll(totalflowDF3).unionAll(totalflowDF2).unionAll(totalflowDF1)
      .unionAll(activeusersDF4).unionAll(activeusersDF3).unionAll(activeusersDF2).unionAll(activeusersDF1)
      .unionAll(sessionsDF4).unionAll(sessionsDF3).unionAll(sessionsDF2).unionAll(sessionsDF1)
      .repartition(20).write.mode(SaveMode.Overwrite).format("orc").save(outputResult)


  }
}
