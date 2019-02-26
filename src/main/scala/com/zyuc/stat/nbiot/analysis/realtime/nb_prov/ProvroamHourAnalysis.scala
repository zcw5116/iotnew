package com.zyuc.stat.nbiot.analysis.realtime.nb_prov

import com.zyuc.iot.utils.DbUtils
import com.zyuc.stat.nbiot.analysis.realtime.utils.CommonUtils
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by liuzk on 18-11-01 下午14:29.
  * 签约省 +regcity 接入省 +city 在线数
  */
object ProvroamHourAnalysis {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()//.setMaster("local[2]").setAppName("opennum_2018101010")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)

    val appName = sc.getConf.get("spark.app.name")
    val inputPath_3g = sc.getConf.get("spark.app.inputPath", "/user/iot/data/provroam/3g/")
    val inputPath_4g = sc.getConf.get("spark.app.inputPath", "/user/iot/data/provroam/4g/")
    val inputPath_nb = sc.getConf.get("spark.app.inputPath", "/user/iot/data/provroam/nb/")
    val outputPath = sc.getConf.get("spark.app.outputPath", "/user/iot/data/provroam/data")

    val dataTime = appName.substring(appName.lastIndexOf("_") + 1)
    val dayTime = dataTime.substring(0, 8)

    import sqlContext.implicits._
    def opennumETL(inputPath: String)={
      val dataDF = sc.textFile(inputPath + "*/*"+dataTime+"*").map(x=>x.split(",")).filter(_.length==6)
        .filter(x=>x(1).length>1).map(x=>(x(1), x(2), x(3), x(4), x(5)))
        .toDF("regprovince", "regcity", "province", "city", "gather_value")

      val resultDF = dataDF.withColumn("gather_cycle", lit(dataTime + "0000")).
        withColumn("gather_date", lit(dataTime.substring(0,8))).
        withColumn("gather_time", lit(dataTime.substring(8,10) + "0000"))

      val onlineDF = resultDF.selectExpr("gather_cycle","gather_date", "gather_time",
        "regprovince", "regcity", "province", "city",
        "'ONLINEUSER' as gather_type", "gather_value")

      onlineDF
    }

    val df3g = opennumETL(inputPath_3g)
    val df4g = opennumETL(inputPath_4g)
    val dfnb = opennumETL(inputPath_nb)
    val outputResult_3g = outputPath + "/3g"
    val outputResult_4g = outputPath + "/4g"
    val outputResult_nb = outputPath + "/nb"
    df3g.write.mode(SaveMode.Overwrite).format("orc").save(outputResult_3g)
    df4g.write.mode(SaveMode.Overwrite).format("orc").save(outputResult_4g)
    dfnb.write.mode(SaveMode.Overwrite).format("orc").save(outputResult_nb)
    insertTIDB("iot_ana_5min_3g_roam_prov_stat",outputResult_3g)
    insertTIDB("iot_ana_5min_4g_roam_prov_stat",outputResult_4g)
    insertTIDB("iot_ana_5min_nb_roam_prov_stat",outputResult_nb)

    def insertTIDB(whichtable: String,whichpath: String)= {
      // 将结果写入到tidb, 需要调整为upsert
      var dbConn = DbUtils.getDBConnection
      dbConn.setAutoCommit(false)
      val sql =
        s"""
           |insert into ${whichtable}_${dayTime}
           |(gather_cycle, gather_date, gather_time, regprovince, regcity, province, city, gather_type, gather_value)
           |values (?,?,?,?,?,?,?,?,?)
           |on duplicate key update gather_value=?
         """.stripMargin

    val pstmt = dbConn.prepareStatement(sql)
    val result = sqlContext.read.format("orc").load(whichpath).map(x=>(x.getString(0), x.getString(1),
      x.getString(2), x.getString(3),x.getString(4), x.getString(5), x.getString(6), x.getString(7), x.getString(8))).collect()

    var i =0
    for(r<-result){
      val gather_cycle = r._1
      val gather_date = r._2
      val gather_time = r._3
      val regprovince = r._4
      val regcity = r._5
      val province = r._6
      val city = r._7
      val gather_type = r._8
      val gather_value = r._9

      pstmt.setString(1, gather_cycle)
      pstmt.setString(2, gather_date)
      pstmt.setString(3, gather_time)
      pstmt.setString(4, regprovince)
      pstmt.setString(5, regcity)
      pstmt.setString(6, province)
      pstmt.setString(7, city)
      pstmt.setString(8, gather_type)
      pstmt.setString(9, gather_value)
      pstmt.setString(10, gather_value)

      i += 1
      pstmt.addBatch()
      if (i % 1000 == 0) {
        pstmt.
          executeBatch
        dbConn.commit()
      }
    }
    pstmt.executeBatch
      dbConn.commit()
    pstmt.close()
    dbConn.close()

    // 更新断点时间
    CommonUtils.updateBreakTable(s"${whichtable}", dataTime)
      }
  }
}

