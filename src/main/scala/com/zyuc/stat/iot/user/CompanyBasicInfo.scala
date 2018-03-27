package com.zyuc.stat.iot.user

import com.zyuc.stat.iot.analysis.util.HbaseDataUtil
import com.zyuc.stat.properties.ConfigProperties
import com.zyuc.stat.utils.HbaseUtils
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by zhoucw on 17-10-6.
  */
object CompanyBasicInfo {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    //.setMaster("local[4]").setAppName("fd")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    sqlContext.sql("use " + ConfigProperties.IOT_HIVE_DATABASE)

    val userTablePartitionID = sc.getConf.get("spark.app.table.userTablePartitionDayID", "20170922")
    val userTable = sc.getConf.get("spark.app.table.userTable", "iot_basic_userinfo") //"iot_basic_userinfo"
    val userAndDomainTable = sc.getConf.get("spark.app.table.userAndDomain", "iot_basic_user_and_domain") //"iot_basic_user_and_domain"
    val companyAndDomain = sc.getConf.get("spark.app.table.companyAndDomain", "iot_basic_company_and_domain") //"iot_basic_company_and_domain"
    val companyNameFile = sc.getConf.get("spark.app.companyNameFile", "/hadoop/IOT/ANALY_PLATFORM/BasicData/BaseInfo/map_iot_company.yml")//
    val companyHtable = sc.getConf.get("spark.app.htable.companyHtable", "iot_company_basics")
    val companyVpdnHtable = sc.getConf.get("spark.app.htable.companyVpdnHtable", "iot_company_split")
    // val ifUpdateLatestInfo = sc.getConf.get("spark.app.ifUpdateLatestInfo","1") //  是否更新iot_basic_companyinfo的latestdate和cnt_latest, 0-不更新， 1-更新
    val appName = sc.getConf.get("spark.app.name")


    val companyFamilies = new Array[String](2)
    companyFamilies(0) = "basicinfo"
    companyFamilies(1) = "cardcnt"

    val companyVpdnFamilies = new Array[String](2)
    companyVpdnFamilies(0) = "basicinfo"
    companyVpdnFamilies(1) = "cardcnt"

    HbaseUtils.createIfNotExists(companyHtable, companyFamilies)
    HbaseUtils.createIfNotExists(companyVpdnHtable, companyVpdnFamilies)

    // 关联企业名称
    import sqlContext.implicits._
    val companyNameDF = sqlContext.read.format("text").load(companyNameFile).map(x=>x.getString(0).replaceAll(" ","").split(":", 2)).map(x=>(x(0).trim, x(1).trim)).toDF("companyname", "companycode")
    val companNameDimTable = "companNameDimTable"
    companyNameDF.registerTempTable(companNameDimTable)

    val companyTable = "companyTable_" + userTablePartitionID
    sqlContext.sql(
      s"""
         |cache table ${companyTable} as
         |select t.provincecode, t.provincename, t.companycode, d.companyname, t.vpdndomain, t.flag
         |from
         |(
         |select provincecode, provincename, companycode, vpdndomain, flag
         |from
         |(
         |    select provincecode, provincename, companycode, flag, (case when length(vpdndomain)=0 then null else vpdndomain end) as vpdndomain,
         |           row_number() over(partition by companycode) rn
         |    from ${companyAndDomain} where d='${userTablePartitionID}'
         |) c where c.rn=1
         |) t left join ${companNameDimTable} d
         |on(t.companycode=d.companycode)
       """.stripMargin)

    val userCompanyTmpTable = appName + "_userCompanyTmpTable"
    val userCompanyDF = sqlContext.sql(
      s"""select companycode, count(*) as usernum,
         |sum(case when isdirect='1' then 1 else 0 end) dnum,
         |sum(case when isvpdn='1' then 1 else 0 end) cnum,
         |sum(case when iscommon='1' then 1 else 0 end) pnum
         |from $userTable
         |where d='$userTablePartitionID'
         |group by  companycode
       """.stripMargin).registerTempTable(userCompanyTmpTable)

    val resultDF = sqlContext.sql(
      s"""select c.provincecode, c.provincename,
         |c.companycode, c.companyname, c.vpdndomain, nvl(u.usernum,0) usernum,
         |nvl(u.dnum,0) dnum, nvl(u.cnum,0) cnum, nvl(u.pnum,0) pnum,
         |(case when u.usernum>=1000 then 'A' when u.usernum>=200 then 'B' else 'C' end) alevel,
         |(case when u.dnum>=1000 then 'A' when u.dnum>=200 then 'B' else 'C' end) dlevel,
         |(case when u.cnum>=1000 then 'A' when u.cnum>=200 then 'B' else 'C' end) clevel,
         |(case when u.pnum>=1000 then 'A' when u.pnum>=200 then 'B' else 'C' end) plevel,
         |(case when u.dnum>0 then 1 else 0 end) dflag,
         |(case when u.cnum>0 then 1 else 0 end) cflag,
         |c.flag
         |from  $companyTable c left join   $userCompanyTmpTable u
         |on(u.companycode=c.companycode)
       """.stripMargin)

    val companyRDD = resultDF.coalesce(1).rdd.map(x=>{
      val procode = if (null == x(0) || ""==x(0).toString) "-1" else x(0).toString
      val proname = if (null == x(1) || ""==x(1).toString) "-1" else x(1).toString
      val comcode = if (null == x(2) || ""==x(2).toString) "-1" else x(2).toString
      val comName = if(null == x(3) || ""==x(3).toString) "-1" else x(3).toString
      val domain = if (null == x(4) || ""==x(4).toString) "-1" else x(4).toString
      val usernum = x(5).toString
      val dnum = x(6).toString
      val cnum = x(7).toString
      val pnum = x(8).toString
      val alevel = x(9).toString
      val dlevel = x(10).toString
      val clevel = x(11).toString
      val plevel = x(12).toString
      val dflag = x(13).toString
      val cflag = x(14).toString
      val flag = x(15).toString  // 活跃用户的标志

      val put = new Put(Bytes.toBytes(comcode))
      val aPut = new Put(Bytes.toBytes(comcode + "_-1_-1"))
      val dPut = new Put(Bytes.toBytes(comcode + "_D_-1" ))
      val cPut = new Put(Bytes.toBytes(comcode + "_C_-1" ))
      val pPut = new Put(Bytes.toBytes(comcode + "_P_-1" ))

      put.addColumn(Bytes.toBytes("basicinfo"), Bytes.toBytes("provincecode"), Bytes.toBytes(procode))
      put.addColumn(Bytes.toBytes("basicinfo"), Bytes.toBytes("provincename"), Bytes.toBytes(proname))
      put.addColumn(Bytes.toBytes("basicinfo"), Bytes.toBytes("companyname"), Bytes.toBytes(comName))
      put.addColumn(Bytes.toBytes("basicinfo"), Bytes.toBytes("vpdndomain"), Bytes.toBytes(domain))
      put.addColumn(Bytes.toBytes("basicinfo"), Bytes.toBytes("dflag"), Bytes.toBytes(dflag))
      put.addColumn(Bytes.toBytes("basicinfo"), Bytes.toBytes("cflag"), Bytes.toBytes(cflag))
      put.addColumn(Bytes.toBytes("basicinfo"), Bytes.toBytes("monilevel_autocalc"), Bytes.toBytes(alevel))
      put.addColumn(Bytes.toBytes("basicinfo"), Bytes.toBytes("flag"), Bytes.toBytes(flag))
      put.addColumn(Bytes.toBytes("cardcnt"), Bytes.toBytes("cnt_latest"), Bytes.toBytes(usernum))
      put.addColumn(Bytes.toBytes("cardcnt"), Bytes.toBytes("date_latest"), Bytes.toBytes(userTablePartitionID.toString))
      put.addColumn(Bytes.toBytes("cardcnt"), Bytes.toBytes("cnt_" + userTablePartitionID.toString), Bytes.toBytes(usernum))

      aPut.addColumn(Bytes.toBytes("basicinfo"), Bytes.toBytes("provincecode"), Bytes.toBytes(procode))
      aPut.addColumn(Bytes.toBytes("basicinfo"), Bytes.toBytes("provincename"), Bytes.toBytes(proname))
      aPut.addColumn(Bytes.toBytes("basicinfo"), Bytes.toBytes("companyname"), Bytes.toBytes(comName))
      aPut.addColumn(Bytes.toBytes("basicinfo"), Bytes.toBytes("vpdndomain"), Bytes.toBytes(domain))
      aPut.addColumn(Bytes.toBytes("basicinfo"), Bytes.toBytes("monilevel_autocalc"), Bytes.toBytes(alevel))
      aPut.addColumn(Bytes.toBytes("basicinfo"), Bytes.toBytes("flag"), Bytes.toBytes(flag))
      aPut.addColumn(Bytes.toBytes("cardcnt"), Bytes.toBytes("cnt_latest"), Bytes.toBytes(usernum))
      aPut.addColumn(Bytes.toBytes("cardcnt"), Bytes.toBytes("date_latest"), Bytes.toBytes(userTablePartitionID.toString))
      aPut.addColumn(Bytes.toBytes("cardcnt"), Bytes.toBytes("cnt_" + userTablePartitionID.toString), Bytes.toBytes(usernum))

      dPut.addColumn(Bytes.toBytes("basicinfo"), Bytes.toBytes("provincecode"), Bytes.toBytes(procode))
      dPut.addColumn(Bytes.toBytes("basicinfo"), Bytes.toBytes("provincename"), Bytes.toBytes(proname))
      dPut.addColumn(Bytes.toBytes("basicinfo"), Bytes.toBytes("companyname"), Bytes.toBytes(comName))
      dPut.addColumn(Bytes.toBytes("basicinfo"), Bytes.toBytes("vpdndomain"), Bytes.toBytes(domain))
      dPut.addColumn(Bytes.toBytes("basicinfo"), Bytes.toBytes("monilevel_autocalc"), Bytes.toBytes(dlevel))
      dPut.addColumn(Bytes.toBytes("basicinfo"), Bytes.toBytes("flag"), Bytes.toBytes(flag))
      dPut.addColumn(Bytes.toBytes("cardcnt"), Bytes.toBytes("cnt_latest"), Bytes.toBytes(dnum))
      dPut.addColumn(Bytes.toBytes("cardcnt"), Bytes.toBytes("date_latest"), Bytes.toBytes(userTablePartitionID.toString))
      dPut.addColumn(Bytes.toBytes("cardcnt"), Bytes.toBytes("cnt_" + userTablePartitionID.toString), Bytes.toBytes(dnum))

      cPut.addColumn(Bytes.toBytes("basicinfo"), Bytes.toBytes("provincecode"), Bytes.toBytes(procode))
      cPut.addColumn(Bytes.toBytes("basicinfo"), Bytes.toBytes("provincename"), Bytes.toBytes(proname))
      cPut.addColumn(Bytes.toBytes("basicinfo"), Bytes.toBytes("companyname"), Bytes.toBytes(comName))
      cPut.addColumn(Bytes.toBytes("basicinfo"), Bytes.toBytes("monilevel_autocalc"), Bytes.toBytes(clevel))
      cPut.addColumn(Bytes.toBytes("basicinfo"), Bytes.toBytes("flag"), Bytes.toBytes(flag))
      cPut.addColumn(Bytes.toBytes("cardcnt"), Bytes.toBytes("cnt_latest"), Bytes.toBytes(cnum))
      cPut.addColumn(Bytes.toBytes("cardcnt"), Bytes.toBytes("date_latest"), Bytes.toBytes(userTablePartitionID.toString))
      cPut.addColumn(Bytes.toBytes("cardcnt"), Bytes.toBytes("cnt_" + userTablePartitionID.toString), Bytes.toBytes(cnum))

      pPut.addColumn(Bytes.toBytes("basicinfo"), Bytes.toBytes("provincecode"), Bytes.toBytes(procode))
      pPut.addColumn(Bytes.toBytes("basicinfo"), Bytes.toBytes("provincename"), Bytes.toBytes(proname))
      pPut.addColumn(Bytes.toBytes("basicinfo"), Bytes.toBytes("companyname"), Bytes.toBytes(comName))
      pPut.addColumn(Bytes.toBytes("basicinfo"), Bytes.toBytes("monilevel_autocalc"), Bytes.toBytes(plevel))
      pPut.addColumn(Bytes.toBytes("basicinfo"), Bytes.toBytes("flag"), Bytes.toBytes(flag))
      pPut.addColumn(Bytes.toBytes("cardcnt"), Bytes.toBytes("cnt_latest"), Bytes.toBytes(pnum))
      pPut.addColumn(Bytes.toBytes("cardcnt"), Bytes.toBytes("date_latest"), Bytes.toBytes(userTablePartitionID.toString))
      pPut.addColumn(Bytes.toBytes("cardcnt"), Bytes.toBytes("cnt_" + userTablePartitionID.toString), Bytes.toBytes(pnum))

      ((new ImmutableBytesWritable, put), (new ImmutableBytesWritable, aPut),(new ImmutableBytesWritable, dPut),(new ImmutableBytesWritable, cPut),(new ImmutableBytesWritable, pPut))
    })
    HbaseDataUtil.saveRddToHbase(companyHtable, companyRDD.map(_._1))
    HbaseDataUtil.saveRddToHbase(companyVpdnHtable, companyRDD.map(_._2))
    HbaseDataUtil.saveRddToHbase(companyVpdnHtable, companyRDD.map(_._3))
    HbaseDataUtil.saveRddToHbase(companyVpdnHtable, companyRDD.map(_._4))
    HbaseDataUtil.saveRddToHbase(companyVpdnHtable, companyRDD.map(_._5))


    val companyDomainDF = sqlContext.sql(
      s"""
         |select c.provincecode, c.provincename, c.companycode, c.companyname, m.vpdndomain, m.usernum,
         |(case when m.usernum>=1000 then 'A' when m.usernum>=200 then 'B' else 'C' end) monilevel,
         |c.flag
         |from
         |(
         |    select companycode, vpdndomain, count(*) usernum
         |    from ${userAndDomainTable} u where u.isvpdn='1'
         |    group by companycode, vpdndomain
         |) m, $companyTable c
         |where m.companycode=c.companycode
       """.stripMargin)

    val companyDomainRDD = companyDomainDF.filter("length(vpdndomain)>0").coalesce(1).rdd.map(x=>{
      val procode = if(null == x(0)) "-1" else x(0).toString
      val proname = if(null == x(1)) "-1" else x(1).toString
      val comcode = if (null == x(2) || ""== x(2)) "-1" else x(2).toString
      val comName = if (null == x(3) || ""== x(3)) "-1" else x(3).toString
      val domain = if(null == x(4) || ""== x(4)) "-1" else x(4).toString
      val usernum = x(5).toString
      val level = x(6).toString
      val flag = x(7).toString
      val rowkey = comcode + "_" + "C" + "_" + domain
      val vpdnPut = new Put(Bytes.toBytes(rowkey))

      vpdnPut.addColumn(Bytes.toBytes("basicinfo"), Bytes.toBytes("provincecode"), Bytes.toBytes(procode))
      vpdnPut.addColumn(Bytes.toBytes("basicinfo"), Bytes.toBytes("provincename"), Bytes.toBytes(proname))
      vpdnPut.addColumn(Bytes.toBytes("basicinfo"), Bytes.toBytes("companycode"), Bytes.toBytes(comcode))
      vpdnPut.addColumn(Bytes.toBytes("basicinfo"), Bytes.toBytes("companyname"), Bytes.toBytes(comName))
      vpdnPut.addColumn(Bytes.toBytes("basicinfo"), Bytes.toBytes("vpdndomain"), Bytes.toBytes(domain))
      vpdnPut.addColumn(Bytes.toBytes("basicinfo"), Bytes.toBytes("monilevel_autocalc"), Bytes.toBytes(level))
      vpdnPut.addColumn(Bytes.toBytes("basicinfo"), Bytes.toBytes("flag"), Bytes.toBytes(flag))
      vpdnPut.addColumn(Bytes.toBytes("cardcnt"), Bytes.toBytes("cnt_latest"), Bytes.toBytes(usernum))
      vpdnPut.addColumn(Bytes.toBytes("cardcnt"), Bytes.toBytes("date_latest"), Bytes.toBytes(userTablePartitionID.toString))
      vpdnPut.addColumn(Bytes.toBytes("cardcnt"), Bytes.toBytes("cnt_" + userTablePartitionID.toString), Bytes.toBytes(usernum))

      (new ImmutableBytesWritable, vpdnPut)
    })
    HbaseDataUtil.saveRddToHbase(companyVpdnHtable, companyDomainRDD)


  }

}
