package com.zyuc.stat.iot.analysis.common

import com.zyuc.stat.iot.analysis.util.{HbaseDataUtil, OnlineHtableConverter}
import com.zyuc.stat.properties.ConfigProperties
import com.zyuc.stat.utils.DateUtils.timeCalcWithFormatConvertSafe
import com.zyuc.stat.utils.{DateUtils, HbaseUtils}
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{Logging, SparkConf, SparkContext}

/**
  * Created by zhoucw on 17-10-8.
  */
object TempAnalysis extends Logging{
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    sqlContext.sql("use " + ConfigProperties.IOT_HIVE_DATABASE)

    // 获取参数
    val appName = sc.getConf.get("spark.app.name") // name_201708010040

    val userTmp= sc.getConf.get("spark.app.table.usertmp", "tmpuser_js")
    val hbaseTmp = sc.getConf.get("spark.app.htable.hbaseTmp", "hbasetmp_js")

    /////////////////////////////////////////////////////////////////////////////////////////
    //  Hbase 相关的表
    //  表不存在， 就创建
    /////////////////////////////////////////////////////////////////////////////////////////
    //  curAlarmHtable-当前时刻的预警表,  nextAlarmHtable-下一时刻的预警表,
    val families = new Array[String](1)
    families(0) = "f"
    HbaseUtils.createIfNotExists(hbaseTmp, families)


    val d = appName.substring(appName.indexOf("_") + 3, appName.indexOf("_") + 9 )
    val pdsnInput = "/hadoop/IOT/data/cdr/ETL/day/pdsn/data/d=" + d
    val pgwInput = "/hadoop/IOT/data/cdr/ETL/day/pgw/data/d=" + d

    val pdsnDataDF = sqlContext.read.format("orc").load(pdsnInput)
    val pgwDataDF = sqlContext.read.format("orc").load(pgwInput)

    val pdsnTable = "pdsnTable_" + d
    val pgwTable = "pgwTable_" + d

    pdsnDataDF.registerTempTable(pdsnTable)
    pgwDataDF.registerTempTable(pgwTable)

    val pdsnStatSQL =
      s"""
         |select u.mdn, u.custtype, u.companycode from ${userTmp} u left semi join ${pdsnTable} p on(u.mdn = p.mdn)
       """.stripMargin

    val pdsnResultDF = sqlContext.sql(pdsnStatSQL)
    val pdsnRDD = pdsnResultDF.coalesce(5).rdd.map(x=>{

      val rowkey = if(null == x(0)) "-1" else x(0).toString
      val custtype = if(null == x(1)) "-1" else x(1).toString
      val companycode = if(null == x(2)) "-1" else x(2).toString

      val put = new Put(Bytes.toBytes(rowkey))
      put.addColumn(Bytes.toBytes("f"), Bytes.toBytes("custtype"), Bytes.toBytes(custtype))
      put.addColumn(Bytes.toBytes("f"), Bytes.toBytes("companycode"), Bytes.toBytes(companycode))

      (new ImmutableBytesWritable, put)
    })

    HbaseDataUtil.saveRddToHbase(hbaseTmp, pdsnRDD)

    val pgwStatSQL =
      s"""
         |select u.mdn, u.custtype, u.companycode from ${userTmp} u left semi join ${pgwTable} p on(u.mdn = p.mdn)
       """.stripMargin

    val pgwtDF = sqlContext.sql(pgwStatSQL)


    val pgwRDD = pgwtDF.coalesce(5).rdd.map(x=>{

      val rowkey = if(null == x(0)) "-1" else x(0).toString
      val custtype = if(null == x(1)) "-1" else x(1).toString
      val companycode = if(null == x(2)) "-1" else x(2).toString

      val put = new Put(Bytes.toBytes(rowkey))
      put.addColumn(Bytes.toBytes("f"), Bytes.toBytes("custtype"), Bytes.toBytes(custtype))
      put.addColumn(Bytes.toBytes("f"), Bytes.toBytes("companycode"), Bytes.toBytes(companycode))

      (new ImmutableBytesWritable, put)
    })

    HbaseDataUtil.saveRddToHbase(hbaseTmp, pgwRDD)

  }




}
