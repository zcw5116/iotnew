package com.zyuc.stat.nbiot.analysis.realtime.nb_prov

import com.zyuc.iot.utils.DbUtils
import com.zyuc.stat.nbiot.analysis.realtime.utils.CommonUtils
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by liuzk on 18-5-11 下午10:29.
  */
object NbCdrM5Analysis_prov {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()//.setMaster("local[2]").setAppName("NbM5Analysis_201805161510")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)

    val appName = sc.getConf.get("spark.app.name")
    val inputPath = sc.getConf.get("spark.app.inputPath", "/user/iot/data/cdr/transform/nb/data")
    val outputPath = sc.getConf.get("spark.app.outputPath", "/user/iot/data/cdr/analy_realtime/nb_prov")

    val repartitionNum = sc.getConf.get("spark.app.repartitionNum", "10").toInt


    val dataTime = appName.substring(appName.lastIndexOf("_") + 1)
    val d= dataTime.substring(2,8)
    val h= dataTime.substring(8, 10)
    val m5 = dataTime.substring(10, 12)
    val partitionPath = s"/d=$d/h=$h/m5=$m5"

    val cdrTempTable = "CDRTempTable"
    sqlContext.read.format("orc").load(inputPath + partitionPath)
      .selectExpr("prov","city","enbid","t805","l_datavolumefbcuplink as upflow","l_datavolumefbcdownlink as downflow")
      .filter("prov='河北'")
      .registerTempTable(cdrTempTable)

    // 基站
    val iotBSInfoPath = sc.getConf.get("spark.app.IotBSInfoPath", "/user/iot/data/basic/IotBSInfo/data/")
    val bsInfoTable = "IOTBSInfoTable"
    sqlContext.read.format("orc").load(iotBSInfoPath).registerTempTable(bsInfoTable)

    val cdrBSTable = "cdrBSTable"
    sqlContext.sql(
      s"""
         |select b.provname as prov, nvl(b.cityname,'未知') as city, c.enbid,b.zhLabel,
         |        c.upflow, c.downflow
         |from ${cdrTempTable} c
         |left join ${bsInfoTable} b
         |on(c.enbid = b.enbid and c.prov=b.provname)
       """.stripMargin).registerTempTable(cdrBSTable)

    val df = sqlContext.sql(
      s"""
         |select prov, city, enbid,
         |        sum(upflow) as upflow, sum(downflow) as downflow, sum(upflow+downflow) as totalflow,
         |        nvl(zhLabel,"未知") as zhLabel
         |from ${cdrBSTable}
         |group by prov, city, enbid, zhLabel
       """.stripMargin)

    val resultDF = df.
      withColumn("gather_cycle", lit(dataTime + "00")).
      withColumn("gather_date", lit(dataTime.substring(0,8))).
      withColumn("gather_time", lit(dataTime.substring(8,12) + "00"))

    val inflowDF = resultDF.selectExpr("gather_cycle","gather_date", "gather_time", "prov", "city","enbid","zhLabel", "upflow as gather_value").
      withColumn("gather_type", lit("OUTFLOW"))
    val outflowDF = resultDF.selectExpr("gather_cycle","gather_date", "gather_time", "prov", "city", "enbid","zhLabel", "downflow as gather_value").
      withColumn("gather_type", lit("INFLOW"))
    val totalflowDF = resultDF.selectExpr("gather_cycle","gather_date", "gather_time", "prov", "city", "enbid","zhLabel", "totalflow as gather_value").
      withColumn("gather_type", lit("TOTALFLOW"))

    // 将结果写入到hdfs
    val outputResult = outputPath + partitionPath
    inflowDF.unionAll(outflowDF).unionAll(totalflowDF).filter("gather_value!=0").coalesce(1)
      .write.mode(SaveMode.Overwrite).format("orc").save(outputResult)


    // 将结果写入到tidb, 需要调整为upsert
    var dbConn = DbUtils.getDBConnection
    dbConn.setAutoCommit(false)
    val sql =
      s"""
         |insert into iot_ana_5min_prov_nb_stat
         |(gather_cycle, gather_date, gather_time, btsid, province,city,town, region,bts, gather_type, gather_value)
         |values (?,?,?,?,?,?,?,?,?,?,?)
         |on duplicate key update gather_value=?
       """.stripMargin

    val pstmt = dbConn.prepareStatement(sql)
    val result = sqlContext.read.format("orc").load(outputResult).filter("prov='河北'").coalesce(10).
      map(x=>(x.getString(0), x.getString(1), x.getString(2), x.getString(3),
        x.getString(4), x.getString(5),x.getString(6), x.getDouble(7), x.getString(8))).collect()
    //x.getLong(7)  -> x.getDouble(7)
    var i = 0
    for(r<-result){
      val gather_cycle = r._1
      val gather_date = r._2
      val gather_time = r._3
      val prov = r._4
      val city = r._5
      val btsid = r._6
      val bts = r._7
      val gather_type = r._9
      val gather_value = r._8

      pstmt.setString(1, gather_cycle)
      pstmt.setString(2, gather_date)
      pstmt.setString(3, gather_time)
      pstmt.setString(4, btsid)
      pstmt.setString(5, prov)
      pstmt.setString(6, city)
      pstmt.setString(7, "未知")
      pstmt.setString(8, "未知")
      pstmt.setString(9, bts)
      pstmt.setString(10, gather_type)
      pstmt.setDouble(11, gather_value)
      pstmt.setDouble(12, gather_value)

      i += 1
      pstmt.addBatch()
      if (i % 1000 == 0) {
        pstmt.executeBatch
        dbConn.commit()
      }
    }
    pstmt.executeBatch
    dbConn.commit()
    pstmt.close()
    dbConn.close()

    // 更新断点时间
    CommonUtils.updateBreakTable("iot_ana_5min_prov_nb_stat_flux", dataTime+"00")

  }
}
