package com.zyuc.stat.nbiot.analysis.day

import java.io.IOException

import com.zyuc.iot.utils.DbUtils
import com.zyuc.stat.nbiot.analysis.realtime.utils.CommonUtils
import com.zyuc.stat.utils.FileUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}


/**
  * Created by liuzk on 18-11-01 下午14:29.
  * 234g NB 分省开户数据
  */
object OpennumDayAnalysis {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()//.setMaster("local[2]").setAppName("OPennum_20181109")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    val fileSystem = FileSystem.get(sc.hadoopConfiguration)

    val appName = sc.getConf.get("spark.app.name")
    val inputPath = sc.getConf.get("spark.app.inputPath", "/user/iot/data/opennum/")
    val outputPath = sc.getConf.get("spark.app.outputPath", "/user/iot/data/opennum/data")

    val dataTime = appName.substring(appName.lastIndexOf("_") + 1)

    import sqlContext.implicits._
    def opennumETL(whichtype: String,nettype: String):DataFrame ={

      val path = new Path(inputPath + dataTime + s"*/iot_prov_opennum_${whichtype}_static." + dataTime + ".ok")

      if (FileUtils.getFilesByWildcard(fileSystem, path.toString).length > 0) {
        val dataDF = sc.textFile(inputPath + dataTime + s"*/iot_prov_opennum_${whichtype}_static." + dataTime + ".ok").map(x=>x.split(",")).filter(_.length==4).
          filter(x=>x(1).length>1).map(x=>(x(1), x(2), x(3))).toDF("province", "city", "gather_value")

        val resultDF = dataDF.withColumn("gather_cycle", lit(dataTime + "000000")).
          withColumn("gather_date", lit(dataTime.substring(0,8)))

        val onlineDF = resultDF.selectExpr("gather_cycle","gather_date", "province", "city",
          "'OPENNUM' as gather_type", s"'${nettype}' as net_type", "gather_value")

        onlineDF
      }else{
        val onlineDF = sqlContext.read.json(inputPath + "opennumTmp.json")
        onlineDF
      }
    }

    val df234g = opennumETL("234g","234G")
    val df23g = opennumETL("23g","23G")
    val df4g = opennumETL("4g","4G")
    val dfnb = opennumETL("nb","NB")

    val df_all = df234g.unionAll(df23g).unionAll(df4g).unionAll(dfnb).filter("gather_cycle!='000'")
    val outputResult = outputPath + "/d=" + dataTime
    df_all.write.mode(SaveMode.Overwrite).format("orc").save(outputResult)

    // 将结果写入到tidb, 需要调整为upsert
    var dbConn = DbUtils.getDBConnection
    dbConn.setAutoCommit(false)
    val sql =
      s"""
         |insert into iot_ana_prov_stat
         |(gather_cycle, gather_date, province, city, gather_type, net_type, gather_value)
         |values (?,?,?,?,?,?,?)
         |on duplicate key update gather_value=?
       """.stripMargin

    val pstmt = dbConn.prepareStatement(sql)
    val result = sqlContext.read.format("orc").load(outputResult).
      map(x=>(x.getString(0), x.getString(1), x.getString(2), x.getString(3),
        x.getString(4), x.getString(5), x.getString(6))).collect()

    var i = 0
    for(r<-result){
      val gather_cycle = r._1
      val gather_date = r._2
      val province = r._3
      val city = r._4
      val gather_type = r._5
      val net_type = r._6
      val gather_value = r._7

      pstmt.setString(1, gather_cycle)
      pstmt.setString(2, gather_date)
      pstmt.setString(3, province)
      pstmt.setString(4, city)
      pstmt.setString(5, gather_type)
      pstmt.setString(6, net_type)
      pstmt.setString(7, gather_value)
      pstmt.setString(8, gather_value)

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
    CommonUtils.updateBreakTable("iot_ana_prov_stat_opennum", dataTime)
  }
}

