package com.zyuc.stat.iot.user

import com.zyuc.stat.properties.ConfigProperties
import com.zyuc.stat.utils.DateUtils.timeCalcWithFormatConvertSafe
import com.zyuc.stat.utils.{FileUtils, HbaseUtils}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{Logging, SparkConf, SparkContext}

import scala.collection.mutable


/**
  * Created by zhoucw on 17-7-9.
  * Desc: 1. 3G在线规则：取话单截断时间在统计时间点至前7个小时区间，状态为非结束状态，排除掉这个区间中话单中状态存在结束状态的account_session
          2. 4G 在线规则： 取话单结束时间在统计时间点至后两个小时区间， 话单开始时间在统计时间点之前的用户
  *@deprecated
  */
object UserOnlineBaseData extends Logging{

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()//.setAppName("UserOnlineBaseData").setMaster("local[4]")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    val hivedb = ConfigProperties.IOT_HIVE_DATABASE
    sqlContext.sql("use " + hivedb)


    val curHourtime = sc.getConf.get("spark.app.hourid") // 2017072412
    val outputPath = sc.getConf.get("spark.app.outputPath") // "/hadoop/IOT/ANALY_PLATFORM/UserOnline/"
    val userTablePartitionID = sc.getConf.get("spark.app.userTablePartitionID")
    val userTable = sc.getConf.get("spark.app.table.userTable","iot_basic_userinfo") //"iot_customer_userinfo"=>'iot_basic_userinfo'
    val pdsnTable = sc.getConf.get("spark.app.table.pdsnTable")
    val pgwTable = sc.getConf.get("spark.app.table.pgwTable")
    val basenumTable = sc.getConf.get("spark.app.table.basenumTable", "iot_useronline_base_nums")
    val whetherUpdateBaseDataTime = sc.getConf.get("spark.app.whetherUpdateBaseDataTime", "Y")

    if(whetherUpdateBaseDataTime != "Y" && whetherUpdateBaseDataTime != "N" ){
      logError("日志类型logType错误, 期望值：Y, N ")
      return
    }

    val last7Hourtime = timeCalcWithFormatConvertSafe(curHourtime, "yyyyMMddHH", -7*60*60, "yyyyMMddHH")
    val next2Hourtime = timeCalcWithFormatConvertSafe(curHourtime, "yyyyMMddHH", 2*60*60, "yyyyMMddHH")
    val dayidOfCurHourtime = curHourtime.substring(2, 8)
    val dayidOflast7Hourtime = last7Hourtime.substring(2, 8)
    val dayidOfNext2Hourtime = next2Hourtime.substring(2, 8)
    val curHourid = curHourtime.substring(8, 10)
    val last7Hourid = last7Hourtime.substring(8, 10)
    val next2Hourid = next2Hourtime.substring(8, 10)
    val curPartDayrid = dayidOfCurHourtime

    val userDF = sqlContext.table(userTable).filter("d=" + userTablePartitionID).selectExpr("mdn", "imsicdma",
        "case when length(belo_prov)=0 or belo_prov is null then '其他' else belo_prov end as custprovince",
        "case when length(companycode)=0 or companycode is null then 'P999999999' else companycode end  as vpdncompanycode" )


    userDF.show(1)
    // 缓存用户的表
    val cachedUserinfoTable = "iot_user_basic_info_cached"
    userDF.cache().registerTempTable(cachedUserinfoTable)


    val cachedCompanyTable = "cachedCompany"
    sqlContext.sql(s"""CACHE TABLE ${cachedCompanyTable} as select distinct custprovince, vpdncompanycode from ${cachedUserinfoTable}""")

    sqlContext.table(cachedCompanyTable).show(1)
    // 0点在线的用户
    var commonSql = ""
    if(dayidOfCurHourtime>dayidOflast7Hourtime){
      commonSql =
        s"""
           |select t.mdn, t.account_session_id, t.acct_status_type
           |from ${pdsnTable} t
           |where t.d='${dayidOflast7Hourtime}' and t.h>='${last7Hourid}'
           |union all
           |select t.mdn, t.account_session_id, t.acct_status_type
           |from ${pdsnTable} t
           |where t.d='${dayidOfCurHourtime}' and t.h<'${curHourid}'
         """.stripMargin
    }else{
      commonSql =
        s"""
           |select t.mdn, t.account_session_id, t.acct_status_type
           |from ${pdsnTable} t
           |where t.d='${dayidOflast7Hourtime}'
           |      and t.h>='${last7Hourid}' and t.h<'${curHourid}'
           |""".stripMargin
    }

    val g3resultsql =
      s""" select r.mdn from
         |(
         |    select t1.mdn, t2.mdn as mdn2 from
         |        (select mdn, account_session_id from ( ${commonSql} ) l1 where l1.acct_status_type<>'2') t1
         |    left join
         |        (select mdn, account_session_id from ( ${commonSql} ) l2 where l2.acct_status_type='2' ) t2
         |    on(t1.mdn=t2.mdn and t1.account_session_id=t2.account_session_id)
         |) r
         |where r.mdn2 is null
       """.stripMargin

 /*   val g3usersql =
      s"""select t1.mdn from
         |    (select t.mdn, t.account_session_id
         |     from iot_cdr_3gaaa_ticket t
         |     where t.acct_status_type<>'2' and t.dayid='${dayidOfLastHourtime}' and t.hourid='${lastHourid}'
         |     ) t1,
         |    (select t.mdn, t.account_session_id
         |    from iot_cdr_3gaaa_ticket t
         |    where t.acct_status_type='2' and t.dayid='${dayidOfCurHourtime}' and t.hourid='${curHourid}'
         |    ) t2
         |where t1.mdn=t2.mdn and t1.account_session_id=t2.account_session_id
         |
       """.stripMargin*/

    val g3tmpuser  = "g3tmpuser" + curHourtime
    sqlContext.sql(g3resultsql).registerTempTable(g3tmpuser)

    val g3onlinecomptable = "g3onlinecomp" + curHourtime

    val g3onlinecompsql =
      s"""CACHE TABLE ${g3onlinecomptable} as select r.vpdncompanycode,count(*) as g3cnt
         |from (
         |select u.vpdncompanycode, u.mdn
         |from ${cachedUserinfoTable} u
         |left semi join
         |${g3tmpuser} t
         |on(t.mdn=u.mdn)
         |) r
         |group by r.vpdncompanycode
       """.stripMargin
    sqlContext.sql(g3onlinecompsql).coalesce(1)

    val timeOfFirstUsageStr = timeCalcWithFormatConvertSafe(curHourtime, "yyyyMMddHH",0, "yyyy-MM-dd HH:mm:ss")
    val pgwonlinecomptable = "pgwonlinecomp" + curHourtime

    var pgwSql =
      s"""
         |select l_timeoffirstusage, mdn
         |from ${pgwTable}
         |where d='${dayidOfCurHourtime}' and h>=${curHourid}  and h<${next2Hourid}
       """.stripMargin
    if(dayidOfNext2Hourtime > dayidOfCurHourtime){
      pgwSql =
        s"""
           |select l_timeoffirstusage, mdn
           |from ${pgwTable}
           |where d='${dayidOfCurHourtime}' and h>=${curHourid}
           |union all
           |select l_timeoffirstusage, mdn
           |from ${pgwTable}
           |where d='${dayidOfNext2Hourtime}' and h<${next2Hourid}
       """.stripMargin
    }

    val pgwTempTable = "pgwTempTable_" + curHourtime
    sqlContext.sql(pgwSql).registerTempTable(pgwTempTable)


    val pgwcompsql =
     s"""CACHE TABLE ${pgwonlinecomptable} as select o.vpdncompanycode, count(*) as pgwcnt
        |from ( SELECT u.mdn, u.vpdncompanycode
        |       FROM ${cachedUserinfoTable} u LEFT SEMI JOIN ${pgwTempTable} t
        |       ON  (u.mdn = t.mdn and t.l_timeoffirstusage < '${timeOfFirstUsageStr}')
        |     ) o
        |group by o.vpdncompanycode
      """.stripMargin
    sqlContext.sql(pgwcompsql).coalesce(1)
    sqlContext.sql(pgwcompsql).show()

/*    val companyonlinesum =
      s"""select '${curHourtime}' as hourtime,c.vpdncompanycode, nvl(t1.g3cnt,0) as g3cnt, nvl(t2.pgwcnt,0) as pgwcnt
         |from ${cachedCompanyTable} c
         |left join ${g3onlinecomptable} t1 on(c.vpdncompanycode=t1.vpdncompanycode)
         |left join ${pgwonlinecomptable} t2 on(c.vpdncompanycode=t2.vpdncompanycode)
       """.stripMargin*/

    // 暂时将g3置为0
    val companyonlinesum =
      s"""select '${curPartDayrid}' as d, '${curHourid}' as h, c.custprovince, c.vpdncompanycode, nvl(t1.g3cnt,0) as g3cnt, nvl(t2.pgwcnt,0) as pgwcnt
         |from ${cachedCompanyTable} c
         |left join ${g3onlinecomptable} t1 on(c.vpdncompanycode=t1.vpdncompanycode)
         |left join ${pgwonlinecomptable} t2 on(c.vpdncompanycode=t2.vpdncompanycode)
       """.stripMargin


    // 外部分区表
    // create table iot_external_useronline_base (vpdncompanycode string, g3cnt int, pgwcnt int) partitioned by (dayid string)
    //  row format delimited
    //  fields terminated by '\t'
    //  location 'hdfs://hadoop11:9000/dir2';

    val fileSystem = FileSystem.get(sc.hadoopConfiguration)


    val partitions = "d,h"

    def getTemplate: String = {
      var template = ""
      val partitionArray = partitions.split(",")
      for (i <- 0 until partitionArray.length)
        template = template + "/" + partitionArray(i) + "=*"
      template // rename original dir
    }

    logInfo("companyonlinesum:" + companyonlinesum)
    sqlContext.sql(companyonlinesum).show()
    sqlContext.sql(companyonlinesum).repartition(1).write.mode(SaveMode.Overwrite).format("orc").partitionBy(partitions.split(","): _*).save(outputPath + s"temp/${curHourtime}")
    val outFiles = fileSystem.globStatus(new Path(outputPath + "temp/" + curHourtime + getTemplate + "/*.orc"))
    val filePartitions = new mutable.HashSet[String]
    for (i <- 0 until outFiles.length) {
      val nowPath = outFiles(i).getPath.toString
      filePartitions.+=(nowPath.substring(0, nowPath.lastIndexOf("/")).replace(outputPath + "temp/" + curHourtime, "").substring(1))
    }

    FileUtils.moveTempFiles(fileSystem, outputPath, curHourtime, getTemplate, filePartitions)


    //sqlContext.read.format("orc").load("/hadoop/IOT/ANALY_PLATFORM/UserOnline/20170709").registerTempTable("tttt")
    //sqlContext.sql("select vpdncompanycode, g3cnt, pgwcnt from tttt where vpdncompanycode='C000000517'").collect().foreach(println)
    val sql = s"alter table ${basenumTable} add IF NOT EXISTS partition(d='$curPartDayrid', h='$curHourid')"
    logInfo("sql:" + sql)
    sqlContext.sql(sql)

    // 写入hbase表
    if(whetherUpdateBaseDataTime == "Y"){
      logInfo("write whetherUpdateBaseDataTime to Hbase Table. ")
      HbaseUtils.updateCloumnValueByRowkey("iot_dynamic_info","rowkey001","onlinebase","baseHourid", curHourtime) // 从habase里面获取
    }

    sc.stop()

    }

}
