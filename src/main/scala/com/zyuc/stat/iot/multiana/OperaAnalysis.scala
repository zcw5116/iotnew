package com.zyuc.stat.iot.multiana

import com.zyuc.stat.iot.etl.util.CommonETLUtils
import com.zyuc.stat.properties.ConfigProperties
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
/**
  * Created by zhoucw on 17-7-25.
  */
object OperaAnalysis {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()//.setAppName("OperalogAnalysis").setMaster("local[4]")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    sqlContext.sql("use " + ConfigProperties.IOT_HIVE_DATABASE)
    val operaDay = sc.getConf.get("spark.app.operaDay")
    val operaTable = sc.getConf.get("spark.app.table.operaTable")
    val appName =  sc.getConf.get("spark.app.name")
    val outputPath = sc.getConf.get("spark.app.outputPath")
    val operAnalysisTable = sc.getConf.get("spark.app.table.operAnalysisTable")


    val operaPartitionD = operaDay.substring(2,8)
    val cachedOperaTable = s"iot_opera_log_cached_$operaDay"
    sqlContext.sql(
      s"""CACHE TABLE ${cachedOperaTable} as
         |select l.mdn, l.logtype, l.opername, l.custprovince, l.vpdncompanycode
         |from ${operaTable} l
         |where l.opername in('open','close') and l.oper_result='成功' and length(mdn)>0 and  l.d = '${operaPartitionD}'
       """.stripMargin)

    val openSql =
      s"""
         |select nvl(l1.custprovince, l2.custprovince) custprovince,
         |       nvl(l1.vpdncompanycode, l2.vpdncompanycode) vpdncompanycode,
         |       nvl(l1.mdn, l2.mdn) mdn,
         |case when l2.mdn is null then '2/3G' when l1.mdn is null then '4G' else '2/3/4G' end as nettype,
         |1 as opennum, 0 as closenum
         |from
         |    (select * from ${cachedOperaTable}  where opername='open' and logtype='HLR') l1
         |    full outer join
         |    (select * from ${cachedOperaTable}  where opername='open' and logtype='HSS') l2
         |    on(l1.mdn = l2.mdn)
       """.stripMargin

    val openDF = sqlContext.sql(openSql)

    val closeSql =
      s"""
         |select nvl(l1.custprovince, l2.custprovince) custprovince,
         |       nvl(l1.vpdncompanycode, l2.vpdncompanycode) vpdncompanycode,
         |       nvl(l1.mdn, l2.mdn) mdn,
         |case when l2.mdn is null then '2/3G' when l1.mdn is null then '4G' else '2/3/4G' end as nettype,
         |0 as opennum, 1 as closenum
         |from
         |    (select * from ${cachedOperaTable}  where opername='close' and logtype='HLR') l1
         |    full outer join
         |    (select * from ${cachedOperaTable}  where opername='close' and logtype='HSS') l2
         |    on(l1.mdn = l2.mdn)
       """.stripMargin

    val closeDF = sqlContext.sql(closeSql)

    val allDF = openDF.unionAll(closeDF)

    val operaTmpTable = "operaTable" + operaDay
    allDF.registerTempTable(operaTmpTable)

    val resultSql =
      s"""
         |select t.custprovince, t.vpdncompanycode, t.nettype,
         |sum(opennum) opennum,
         |sum(closenum) closenum
         |from ${operaTmpTable} t
         |group by t.custprovince, t.vpdncompanycode, t.nettype
       """.stripMargin

    val resultDF = sqlContext.sql(resultSql).withColumn("d", lit(operaDay.substring(2,8)))

    val fileSystem = FileSystem.newInstance(sc.hadoopConfiguration)
    val coalesceNum = 1
    val partitions="d"

    CommonETLUtils.saveDFtoPartition(sqlContext, fileSystem, resultDF, coalesceNum, partitions, operaDay, outputPath, operAnalysisTable, appName)

  }

}
