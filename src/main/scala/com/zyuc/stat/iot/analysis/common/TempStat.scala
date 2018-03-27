package com.zyuc.stat.iot.analysis.common

import com.zyuc.stat.iot.analysis.util.{HbaseDataUtil, MMEBaseStationHtableConverter}
import com.zyuc.stat.properties.ConfigProperties
import com.zyuc.stat.utils.{CharacterEncodeConversion, HbaseUtils}
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{Logging, SparkConf, SparkContext}

/**
  * Created by zhoucw on 17-10-8.
  */
object TempStat extends Logging{
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    sqlContext.sql("use " + ConfigProperties.IOT_HIVE_DATABASE)

    // 获取参数
    val appName = sc.getConf.get("spark.app.name","name_20170901") // name_201708010040

    val hbaseTmp = sc.getConf.get("spark.app.htable.hbaseTmp", "hbasetmp_js")

    val hbaseDF = tmpHtableConverter.convertToDF(sc, sqlContext, hbaseTmp)
    val tmpHtable = "tmpHtable"
    hbaseDF.registerTempTable(tmpHtable)


    val input = sc.getConf.get("spark.app.input", "/hadoop/IOT/data/IotCompanyInfo/srcdata/all_iotcompanyinfo.txt")
    val textRDD =  CharacterEncodeConversion.transfer(sc, input, "GBK")

    sqlContext.read.format("text").load(input)
    import sqlContext.implicits._
    val dataDF = textRDD.map(x=>x.split("\\|", 9)).map(x=>(x(0), x(1), x(2), x(3), x(4), x(5), x(6), x(7), x(8))).
      toDF("companycode", "companyname", "domain", "provid", "provcode", "provfullname", "provname", "regionid", "regionname")


    val companyinfoTable = "companyinfo"
    dataDF.registerTempTable("companyinfo")

    val statSQL =
      s"""
         |select nvl(h.companycode, '-1') as companycode, nvl(c.companyname, '未知') as companyname,
         |(case h.custtype when '1000' then '政企客户' when '1100' then '公众客户' when '9900' then '其他客户' else '未知' end) as type,
         | count(*) as usrcnt
         |from ${tmpHtable} h left join ${companyinfoTable} c on(h.companycode=c.companycode)
         |group by h.companycode, c.companyname, h.custtype
       """.stripMargin

    val statDF = sqlContext.sql(statSQL)

    statDF.repartition(1).write.mode(SaveMode.Overwrite).format("orc").save("/tmp/tmp/")

  }
}
