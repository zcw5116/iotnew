package com.zyuc.stat.nbiot.analysis.day.AbnormalCard

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.SaveMode
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by liuzk on 18-12-14.
  */
object PgwFluxDayAnalysis {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()//.setMaster("local[2]").setAppName("name_20181214")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    val appName = sc.getConf.get("spark.app.name")
    val inputPath = sc.getConf.get("spark.app.inputPath", "/user/iot_ete/data/cdr/transform/pgw/data")
    val outputPath = sc.getConf.get("spark.app.outputPath","/user/iot/data/cdr/abnormalCard/summ_d/pgw")

    val userPath = sc.getConf.get("spark.app.userPath", "/user/iot/data/baseuser/data/")
    val userDataTime = sc.getConf.get("spark.app.userDataTime", "20180510")

    val fileSystem = FileSystem.get(sc.hadoopConfiguration)
    val dataTime = appName.substring(appName.lastIndexOf("_") + 1)
    val d = dataTime.substring(2, 8)
    val partitionPath = s"/d=$d/h=*/m5=*"

    val cdrTempTable = "CDRTempTable"
    sqlContext.read.format("orc").load(inputPath + partitionPath)
      .selectExpr("mdn","l_datavolumefbcuplink as upflow","l_datavolumefbcdownlink as downflow")
      .registerTempTable(cdrTempTable)

    val userDataPath = userPath + "/d=" + userDataTime
    val userDF = sqlContext.read.format("orc").load(userDataPath).filter("is4g='Y' and beloprov='江苏'").selectExpr("mdn","custid")
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


    //------------先将两表合并完了，保存到hdfs在分析
    sqlContext.sql(
      s"""
         |select custid, c.mdn, upflow, downflow
         |from ${cdrTempTable} c
         |left join ${userTable} u
         |on c.mdn=u.mdn
       """.stripMargin).coalesce(100).write.format("orc").mode(SaveMode.Overwrite).save(outputPath + "/" + d + "/base")

    val baseTable = "baseTable"
    sqlContext.read.format("orc").load(outputPath + "/" + d + "/base")
      .filter("custid is not null and custid!=''").registerTempTable(baseTable)


    val baseFlux = sqlContext.sql(
                      s"""
                         |select custid, avg(upflow) as avgUpflow, avg(downflow) as avgDownflow,
                         |       avg(totalFlow) as avgTotalFlow, count(distinct mdn) as cnt
                         |from
                         |(
                         |select custid, mdn, upflow, downflow, (upflow+downflow) as totalFlow,
                         |       row_number() over(partition by custid order by (upflow+downflow) asc ) rank,
                         |       count(1) over(partition by custid) maxRank
                         |from ${baseTable}
                         |) a
                         |where rank*100/maxRank > 10 and rank*100/maxRank < 90
                         |group by custid
                       """.stripMargin)

    val baseFluxDF = baseFlux.selectExpr("custid", "'4G' as netType", "'日' as anaCycle", s"'${d}' as summ_cycle",
                                          "avgUpflow", "avgDownflow", "avgTotalFlow",
                                          "'-1' as upPacket", "'-1' as downPacket", "'-1' as totalPacket", "cnt")
    //基表保存到hdfs
    baseFluxDF.filter("custid is not null and custid!=''").coalesce(20)
      .write.format("orc").mode(SaveMode.Overwrite).save(outputPath + "/" + d + "/baseFlux")


    //基表部分字段注册成临时表
    val baseFluxTable = "baseFluxTable"
    sqlContext.read.format("orc").load(outputPath + "/" + d + "/baseFlux").filter("cnt>100")
      .selectExpr("custid", "avgTotalFlow").registerTempTable(baseFluxTable)

    val abnormalFlux = sqlContext.sql(
                        s"""
                           |select b.custid, mdn, avg(upflow) as avgUpflow, avg(downflow) as avgDownflow,
                           |       avg(upflow+downflow) as avgFlow, avgTotalFlow
                           |from ${baseTable} u
                           |left join ${baseFluxTable} b on u.custid=b.custid
                           |group by b.custid, mdn, avgTotalFlow
                         """.stripMargin).filter("avgFlow*100/avgTotalFlow<50 or avgFlow*100/avgTotalFlow>150")

    val abnormalFluxDF = abnormalFlux.selectExpr("custid", s"'${d}' as summ_cycle", "mdn",
                                                  "avgUpflow", "avgDownflow", "avgFlow",
                                                  "'-1' as upPacket", "'-1' as downPacket", "'-1' as totalPacket")
    //异常卡流量保存到hdfs
    abnormalFluxDF.coalesce(20).write.format("orc").mode(SaveMode.Overwrite).save(outputPath + "/" + d + "/abnormalFlux")


  }
}
