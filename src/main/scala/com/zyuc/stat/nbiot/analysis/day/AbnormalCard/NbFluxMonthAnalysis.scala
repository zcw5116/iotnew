package com.zyuc.stat.nbiot.analysis.day.AbnormalCard

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by liuzk on 18-12-14.
  * 异常卡：是用原子基表的：nb8点后 pgw6.30后 pdsn7.30后
  */
object NbFluxMonthAnalysis {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()//.setMaster("local[2]").setAppName("name_20181214")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    val appName = sc.getConf.get("spark.app.name")
    val inputPath = sc.getConf.get("spark.app.inputPath", "/user/iot_ete/data/cdr/summ_m/nb/")
    val outputPath = sc.getConf.get("spark.app.outputPath","/user/iot/data/cdr/abnormalCard/summ_m/nb")

    val userPath = sc.getConf.get("spark.app.userPath", "/user/iot/data/baseuser/data/")
    val userDataTime = sc.getConf.get("spark.app.userDataTime", "20180510")

    val fileSystem = FileSystem.get(sc.hadoopConfiguration)
    val dataTime = appName.substring(appName.lastIndexOf("_") + 1)
    //val d = dataTime.substring(2, 8)
    val monthid = dataTime.substring(0, 6)

    sqlContext.read.format("orc").load(inputPath + "monthid=" + monthid)
      .filter("own_provid='江苏'")
      .selectExpr("custid", "mdn","upflow","downflow")
      .coalesce(10).write.format("orc").mode(SaveMode.Overwrite).save(outputPath + "/" + monthid + "/base")

    val baseTable = "baseTable"
    sqlContext.read.format("orc").load(outputPath + "/" + monthid + "/base")
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

    val baseFluxDF = baseFlux.selectExpr("custid", "'NB' as netType", "'月' as anaCycle", s"'${monthid}' as summ_cycle",
      "avgUpflow", "avgDownflow", "avgTotalFlow",
      "'-1' as upPacket", "'-1' as downPacket", "'-1' as totalPacket", "cnt")
    //基表保存到hdfs
    baseFluxDF.filter("custid is not null and custid!=''").coalesce(10)
      .write.format("orc").mode(SaveMode.Overwrite).save(outputPath + "/" + monthid + "/baseFlux")


    //基表部分字段注册成临时表
    val baseFluxTable = "baseFluxTable"
    sqlContext.read.format("orc").load(outputPath + "/" + monthid + "/baseFlux").filter("cnt>100")
      .selectExpr("custid", "avgTotalFlow").registerTempTable(baseFluxTable)

    val abnormalFlux = sqlContext.sql(
      s"""
         |select b.custid, mdn, avg(upflow) as avgUpflow, avg(downflow) as avgDownflow,
         |       avg(upflow+downflow) as avgFlow, avgTotalFlow
         |from ${baseTable} u
         |left join ${baseFluxTable} b on u.custid=b.custid
         |group by b.custid, mdn, avgTotalFlow
        """.stripMargin).filter("avgFlow*100/avgTotalFlow<50 or avgFlow*100/avgTotalFlow>150")

    val abnormalFluxDF = abnormalFlux.selectExpr("custid", s"'${monthid}' as summ_cycle", "mdn",
      "avgUpflow", "avgDownflow", "avgFlow",
      "'-1' as upPacket", "'-1' as downPacket", "'-1' as totalPacket")
    //异常卡流量保存到hdfs
    abnormalFluxDF.coalesce(10).write.format("orc").mode(SaveMode.Overwrite).save(outputPath + "/" + monthid + "/abnormalFlux")


  }
}
