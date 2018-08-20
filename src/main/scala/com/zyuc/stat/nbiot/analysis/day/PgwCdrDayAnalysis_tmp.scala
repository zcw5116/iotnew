package com.zyuc.stat.nbiot.analysis.day

import java.util.concurrent.{Executors, TimeUnit}

import com.zyuc.iot.utils.DbUtils
import com.zyuc.stat.nbiot.analysis.realtime.utils.CommonUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by liuzk on 18-6-26.
  */
object PgwCdrDayAnalysis_tmp {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()//.setMaster("local[2]").setAppName("name_20180626")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)

    val appName = sc.getConf.get("spark.app.name")
    val inputPath = sc.getConf.get("spark.app.inputPath", "/user/iot_ete/data/cdr/transform/pgw/data")
    val outputPath = sc.getConf.get("spark.app.outputPath","/user/iot/data/cdr/summ_d/pgw")
    //pgw天汇总中间表临时缓存目录
    val tmpCachePath = sc.getConf.get("spark.app.tmpCachePath","/user/iot/data/cdr/summ_d/pgw_tmpCache")
    val userPath = sc.getConf.get("spark.app.userPath", "/user/iot/data/baseuser/data/")
    val userDataTime = sc.getConf.get("spark.app.userDataTime", "20180510")

    val fileSystem = FileSystem.get(sc.hadoopConfiguration)
    val dataTime = appName.substring(appName.lastIndexOf("_") + 1)
    val d = dataTime.substring(2, 8)
    val dd = dataTime.substring(0, 8)
    val partitionPath = s"/d=$d/h=*/m5=*"
    val dayPath = s"/d=$d"

    val cdrTempTable = "CDRTempTable"
    sqlContext.read.format("orc").load(inputPath + partitionPath)
      .selectExpr("mdn")
      .registerTempTable(cdrTempTable)


    val userDataPath = userPath + "/d=" + userDataTime
    val userDF = sqlContext.read.format("orc").load(userDataPath)//.filter("isnb='0'")
    val tmpUserTable = "spark_tmpUser"
    userDF.registerTempTable(tmpUserTable)

    val userTable = "spark_User"
    sqlContext.sql(
      s"""
         |cache table ${userTable}
         |as
         |select mdn, prodtype, beloprov
         |from ${tmpUserTable}
       """.stripMargin)


    // 关联基本信息
    val mdnDF = sqlContext.sql(
      s"""
         |select c.mdn, u.prodtype, u.beloprov
         |from ${cdrTempTable} c
         |inner join ${userTable} u on(c.mdn = u.mdn)
       """.stripMargin)

    mdnDF.repartition(100).write.format("orc").mode(SaveMode.Overwrite).save(tmpCachePath)
    val cdrMdnTable = "spark_cdrmdn"
    sqlContext.read.format("orc").load(tmpCachePath).registerTempTable(cdrMdnTable)
/*    val broadvalues = sc.broadcast(mdnDF)
    val mdndf = broadvalues.valuembai l f
    val cdrMdnTable = "spark_cdrmdn"
    mdndf.registerTempTable(cdrMdnTable)*/




    // 将结果写入到tidb, 需要调整为upsert
    val sql =
      s"""
         |insert into activeUsersTmp
         |(prov, prodtype, cnt)
         |values (?,?,?)
       """.stripMargin



    // 省份
    val provStatDF = sqlContext.sql(
      s"""
         |select  beloprov, "-1" as prodtype, count(distinct mdn) as activeUsers
         |from ${cdrMdnTable}
         |group by beloprov
       """.stripMargin)

    val provPath = "/tmp/active/prov"
    provStatDF.repartition(10).write.format("orc").mode(SaveMode.Overwrite).save(provPath)


    // 省份
    val provProdStatDF = sqlContext.sql(
      s"""
         |select  beloprov, prodtype, count(distinct mdn) as activeUsers
         |from ${cdrMdnTable}
         |group by beloprov, prodtype
       """.stripMargin)

    val provPrdPath = "/tmp/active/provprd"
    provProdStatDF.repartition(10).write.format("orc").mode(SaveMode.Overwrite).save(provPrdPath)




    val result = sqlContext.read.format("orc").load(provPath, provPrdPath).map(
      x=>(x.getString(0), x.getString(1), x.getLong(2))).collect()


    var dbConn = DbUtils.getDBConnection
    dbConn.setAutoCommit(false)
    val pstmt = dbConn.prepareStatement(sql)
    var i = 0
    for(r<-result){
      val prov = r._1
      val prodtype = r._2
      val activeUsers = r._3


      pstmt.setString(1, prov)
      pstmt.setString(2, prodtype)
      pstmt.setLong(3, activeUsers)

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




    // create table activeUsersTmp(prov varchar(30), prodtype varchar(50), cnt bigint(20));


  }
}

