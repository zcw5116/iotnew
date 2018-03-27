package com.zyuc.stat.iot.multiana

import com.zyuc.stat.properties.ConfigProperties
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * Created by zhoucw on 17-8-14.
  */
object CardAnalysisbak {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("OperalogAnalysis")//.setMaster("local[4]")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    sqlContext.sql("use " + ConfigProperties.IOT_HIVE_DATABASE)
    val operaDay = sc.getConf.get("spark.app.operaDay")


  }



  def getOperaDF(sqlContext:SQLContext, operaDay:String):DataFrame = {

    val cachedOperaTable = s"iot_opera_log_cached_$operaDay"
    sqlContext.sql(
      s"""CACHE TABLE ${cachedOperaTable} as
         |select l.mdn, l.logtype, l.opername, l.custprovince, l.vpdncompanycode
         |from iot_opera_log l
         |where l.opername in('open','close') and l.oper_result='成功' and length(mdn)>0 and  l.d = '${operaDay}'
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

    val operaTable = "operaTable" + operaDay
    allDF.registerTempTable(operaTable)


    val resultSql =
      s"""
         |select t.custprovince, t.vpdncompanycode, t.nettype,
         |sum(opennum) opensum,
         |sum(closenum) closesum
         |from ${operaTable} t
         |group by t.custprovince, t.vpdncompanycode, t.nettype
       """.stripMargin

    val resultDF = sqlContext.sql(resultSql)
    resultDF
  }
}
