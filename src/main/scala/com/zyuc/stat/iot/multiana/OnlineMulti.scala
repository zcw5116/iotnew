package com.zyuc.stat.iot.multiana

import com.zyuc.stat.properties.ConfigProperties
import com.zyuc.stat.utils.DateUtils
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by zhoucw on 17-9-4.
  */
object OnlineMulti {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()//.setAppName("UserOnlineBaseData").setMaster("local[4]")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    val hivedb = ConfigProperties.IOT_HIVE_DATABASE
    sqlContext.sql("use " + hivedb)

    val userTablePartitionID = sc.getConf.get("spark.app.userTablePartitionID")
    val userTable = sc.getConf.get("spark.app.table.userTable") //"iot_customer_userinfo"
    val pdsnTable = sc.getConf.get("spark.app.table.pdsnTable")
    val pgwTable = sc.getConf.get("spark.app.table.pgwTable")

    val baseHourid="2017090300"
    val last7HoursAgo = DateUtils.timeCalcWithFormatConvertSafe(baseHourid, "yyyyMMddHH", -7*3600, "yyyyMMddHH")
    val baseDayid = baseHourid.substring(0, 8)
    val dayidOfLast7HoursAgo = last7HoursAgo.substring(0, 8)

    val partDofLast7HoursAgo = last7HoursAgo.substring(2, 8)
    val partHofLast7HoursAgo = last7HoursAgo.substring(8, 10)
    val partDofBase = baseHourid.substring(2, 8)
    val partHofBase = baseHourid.substring(8, 10)



    // 3g 话单PDF
    var pdsnDF:DataFrame = null
    // 取前一天从7个小时之前开始的话单数据
    val pdsnLastDayDF = sqlContext.table(pdsnTable).filter("d=" + partDofLast7HoursAgo).
      filter("h>=" + partHofLast7HoursAgo).select("d", "h", "mdn", "account_session_id", "acct_status_type")
    // 取当前日期到话单数据
    val pdsnCurDayDF = sqlContext.table(pdsnTable).filter("d=" + partDofBase).filter("h<"+partHofBase).
      select("d", "h", "mdn", "account_session_id", "acct_status_type")

    if(partDofBase > partDofLast7HoursAgo){
      pdsnDF = pdsnLastDayDF.unionAll(pdsnCurDayDF)
    }else{
      pdsnDF = pdsnCurDayDF
    }



    val userDF = sqlContext.table(userTable).filter("d=" + userTablePartitionID).select("mdn", "vpdncompanycode")
    val g4DF = sqlContext.table(pgwTable).filter("d=" +  baseDayid.substring(2,8) ).select("mdn", "l_timeoffirstusage", "d", "h")


    calc3GOnlineUser(sc, sqlContext, baseHourid, pdsnDF, userDF)

// iot_cdr_data_pdsn_h

    // calc 3g online usernum

  }

  def calc3GOnlineUser(sc:SparkContext, sqlContext: SQLContext, baseHourid:String, cdr3gDF:DataFrame, userDF:DataFrame) :DataFrame = {

    val last7HoursAgo = DateUtils.timeCalcWithFormatConvertSafe(baseHourid, "yyyyMMddHH", -7*3600, "yyyyMMddHH")
    val baseDayid = baseHourid.substring(0, 8)
    val dayidOfLast7HoursAgo = last7HoursAgo.substring(0, 8)

    val partDofLast7HoursAgo = last7HoursAgo.substring(2, 8)
    val partHofLast7HoursAgo = last7HoursAgo.substring(8, 10)
    val partDofBase = baseHourid.substring(2, 8)
    val partHofBase = baseHourid.substring(8, 10)


    val tempTable = "cdr3gDF_" + baseHourid
    cdr3gDF.registerTempTable(tempTable)

    val g3Sql =
      s"""select  t.mdn, t.account_session_id, t.acct_status_type
         |from ${tempTable} t
         |where t.d>=${partDofLast7HoursAgo} and t.h>=${partHofLast7HoursAgo}
         |      and t.d<=${partDofBase} and t.h<${partHofBase}
       """.stripMargin

    val resultSql =
      s"""select r.mdn from
         |(
         |    select r1.mdn, r2.mdn as mdn2 from
         |    (
         |      select mdn from (${g3Sql}) t1 where t1.acct_status_type<>'2'
         |    ) r1
         |    left join
         |    (
         |      select mdn from (${g3Sql}) t2 where t2.acct_status_type='2'
         |    ) r2
         |    on(r1.mdn = r2.mdn and  and r1.account_session_id=r2.account_session_id)
         |) r where r.mdn2 is null
       """.stripMargin
    val resultDF = sqlContext.sql(resultSql)

    val userTable = "userTable_" + baseHourid
    userDF.registerTempTable(userTable)

    resultDF.join(userDF, resultDF.col("mdn")===userDF.col("mdn")).groupBy(userDF.col("vpdncompanycode")).count()

  }

  def calc4GOnlineUser(sc:SparkContext, sqlContext: SQLContext, baseHourid:String, cdr4gDF:DataFrame, userDF:DataFrame) :DataFrame = {

    userDF
  }

}
