package com.zyuc.stat.iot.etl

import com.zyuc.stat.iot.etl.util.UserInfoConverterUtils
import com.zyuc.stat.properties.ConfigProperties
import com.zyuc.stat.utils.DateUtils
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * Created by zhoucw on 17-9-21.
  */
object UserDataETL {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
      //.setMaster("local[4]").setAppName("fd")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    sqlContext.sql("use " + ConfigProperties.IOT_HIVE_DATABASE)
    val appName = sc.getConf.get("spark.app.appName", "UserInfoETL")
    val dataDayid = sc.getConf.get("spark.app.dataDayid", "20170919")
    val userPrePartionD = sc.getConf.get("spark.app.userPrePartionD", "20170918")  // 增量用户关联时候， 需要关联前一天的全量数据
    // val dataDayid = "20170714"
    // val userTable = "iot_customer_userinfo"
    val syncType = sc.getConf.get("spark.app.syncType", "incr")
    //val inputPath = "/hadoop/IOT/ANALY_PLATFORM/BasicData/UserInfo/"
    val inputPath = sc.getConf.get("spark.app.inputPath", "/hadoop/IOT/ANALY_PLATFORM/BasicData/UserInfo")
    val outputPath = sc.getConf.get("spark.app.outputPath", "/hadoop/IOT/data/basic/user/")
    val userOutputPath = outputPath + "/userInfo/data/d=" +  dataDayid
    val userAndDomainOutputPath = outputPath + "/userAndDomain/data/d=" +  dataDayid
    val companyAndDomainOutputPath = outputPath + "/companyAndDomain/data/d=" +  dataDayid

    val userInfoTable = sc.getConf.get("spark.app.userInfoTable", "iot_basic_userinfo")
    val userAndDomainTable = sc.getConf.get("spark.app.userAndDomainTable", "iot_basic_user_and_domain")
    val companyAndDomainTable = sc.getConf.get("spark.app.companyAndDomainTable", "iot_basic_company_and_domain")

    val provinceMapcodeFile = sc.getConf.get("spark.app.provinceMapcodeFile", "/hadoop/IOT/ANALY_PLATFORM/BasicData/iotDimMapcodeProvince/iot_dim_mapcode_province.txt")
    // val vpnToApnMapFile = sc.getConf.get("spark.app.vpnToApnMapFile", "/hadoop/IOT/ANALY_PLATFORM/BasicData/VpdnToApn/vpdntoapn.txt")


    //val outputPath = "/hadoop/IOT/ANALY_PLATFORM/BasicData/output/UserInfo/"
    val fileWildcard = sc.getConf.get("spark.app.fileWildcard", "all_userinfo_qureyes_20170922*" )
    // val fileWildcard = "all_userinfo_qureyes_20170714*"
    // val fileWildcard = "incr_userinfo_qureyes_20170715*"
    val fileSystem = FileSystem.get(sc.hadoopConfiguration)
    val fileLocation = inputPath + "/" + fileWildcard // "/hadoop/IOT/ANALY_PLATFORM/BasicData/UserInfo/incr_userinfo_qureyes_20170919193212.txt.001.001"
    val crttime = DateUtils.getNowTime("yyyy-MM-dd HH:mm:ss")


    val tmpFileTable = "tmpFileTable_" + dataDayid
    val sourceDF = sqlContext.read.format("text").load(fileLocation)
    val sourceFileDF = sqlContext.createDataFrame(sourceDF.map(x => UserInfoConverterUtils.parseLine(x.getString(0))).filter(_.length != 1), UserInfoConverterUtils.struct)
    sourceFileDF.registerTempTable(tmpFileTable)
    val fileUserDF = sqlContext.sql(
      s"""
         |select mdn, imsicdma, imsilte, companycode, vpdndomain, isvpdn, isdirect, userstatus, atrbprovince,
         |       userprovince, belo_city, belo_prov, custstatus, custtype, prodtype,internetType,vpdnOnly,isCommon
         |from ${tmpFileTable}
       """.stripMargin)
    var userDF = fileUserDF

    if(syncType == "incr"){
      // 用户表， 是否定向业务， 是否vpdn业务
      val incrUserDF = fileUserDF
      val incrUserTable = "incrUserTable_" + dataDayid
      incrUserDF.registerTempTable(incrUserTable)

      val preDayUserTable = "preDayUserTable_" + dataDayid

      sqlContext.sql(
        s"""
           |select mdn, imsicdma, imsilte, companycode, vpdndomain, isvpdn, isdirect, userstatus, atrbprovince,
           |       userprovince, belo_city, belo_prov, custstatus, custtype, prodtype, internetType, vpdnOnly, isCommon
           |from ${userInfoTable}
           |where d='${userPrePartionD}'
       """.stripMargin).registerTempTable(preDayUserTable)

      val resultSQL =
        s"""
           |select  nvl(t.mdn, u.mdn) as mdn,
           |        if(t.mdn is null, u.imsicdma,t.imsicdma) as imsicdma,
           |        if(t.mdn is null, u.imsilte,t.imsilte) as imsilte,
           |        if(t.mdn is null, u.companycode,t.companycode) as companycode,
           |        if(t.mdn is null, u.vpdndomain,t.vpdndomain) as vpdndomain,
           |        if(t.mdn is null, u.isvpdn,t.isvpdn) as isvpdn,
           |        if(t.mdn is null, u.isdirect,t.isdirect) as isdirect,
           |        if(t.mdn is null, u.userstatus,t.userstatus) as userstatus,
           |        if(t.mdn is null, u.atrbprovince,t.atrbprovince) as atrbprovince,
           |        if(t.mdn is null, u.userprovince,t.userprovince) as userprovince,
           |        if(t.mdn is null, u.belo_city,t.belo_city) as belo_city,
           |        if(t.mdn is null, u.belo_prov,t.belo_prov) as belo_prov,
           |        if(t.mdn is null, u.custstatus,t.custstatus) as custstatus,
           |        if(t.mdn is null, u.custtype,t.custtype) as custtype,
           |        if(t.mdn is null, u.prodtype,t.prodtype) as prodtype,
           |        if(t.mdn is null, u.internetType,t.internetType) as internetType,
           |        if(t.mdn is null, u.vpdnOnly,t.vpdnOnly) as vpdnOnly,
           |        if(t.mdn is null, u.isCommon,t.isCommon) as isCommon
           |        from ${preDayUserTable} u full outer join  ${incrUserTable} t
           |        on(u.mdn=t.mdn)
       """.stripMargin

      userDF = sqlContext.sql(resultSQL)

    }

    userDF.repartition(7).write.format("orc").mode(SaveMode.Overwrite).save(userOutputPath)
    sqlContext.sql(s"alter table $userInfoTable add if not exists partition(d='$dataDayid') ")


    // 用户和企业关联表
    val tmpDF = userDF.select("mdn", "imsicdma", "imsilte", "companycode", "vpdndomain", "isvpdn", "isdirect", "userstatus", "atrbprovince",
      "userprovince", "belo_city", "belo_prov", "custstatus", "custtype", "prodtype","internetType","vpdnOnly","isCommon")
    import sqlContext.implicits._
    val userAndDomainAndCompanyDF = tmpDF.rdd.flatMap(line=>{
      val vpdndomain = line(4).toString
      val domainList = vpdndomain.split(",")
      val domainSet = new mutable.HashSet[Tuple19[String, String, String,String, String, String, String, String, String,String, String, String, String, String, String, String, String, String, String]]
      domainList.foreach(e=>{
        val apn = "1"
        domainSet.+=((line(0).toString,line(1).toString,line(2).toString,line(3).toString,e, apn, line(5).toString,line(6).toString,line(7).toString,line(8).toString,
          line(9).toString,line(10).toString,line(11).toString,line(12).toString,line(13).toString,line(14).toString,line(15).toString,line(16).toString,line(17).toString))
      })
      domainSet
    }).toDF("mdn", "imsicdma", "imsilte", "companycode", "vpdndomain", "apn", "isvpdn", "isdirect", "userstatus", "atrbprovince",
      "userprovince", "belo_city", "belo_prov", "custstatus", "custtype", "prodtype","internetType","vpdnOnly","isCommon")
    userAndDomainAndCompanyDF.coalesce(7).write.format("orc").mode(SaveMode.Overwrite).save(userAndDomainOutputPath)
    sqlContext.sql(s"alter table $userAndDomainTable add if not exists partition(d='$dataDayid') ")


    // 企业和域名对应关系表
    val tmpCompanyTable = "tmpCompanyTable"
    userAndDomainAndCompanyDF.select("companycode", "vpdndomain", "belo_prov").distinct().registerTempTable(tmpCompanyTable)

    ////////////////////////////////////////////////////////////////////////////////////
    // 对域名使用正则过滤： 1. fsznjt.vpdn.gd,,dl.vpdn.hn 清洗为： fsznjt.vpdn.gd,dl.vpdn.hn
    //                  2. ,bdhbgl.vpdn.he 清洗为： bdhbgl.vpdn.he
    ////////////////////////////////////////////////////////////////////////////////////
    val tmpDomainAndCompanyDF =  sqlContext.sql(
      s"""select companycode, belo_prov, regexp_replace(regexp_replace(vpdndomain,'^,',''),',{2}',',') as vpdndomain
         |from (
         |select companycode, belo_prov, concat_ws(',',collect_set(vpdndomain)) vpdndomain
         |from ${tmpCompanyTable}
         |group by companycode, belo_prov
         |) m
       """.stripMargin)


    val tmpProvinceMapcodeDF = sqlContext.read.format("text").load(provinceMapcodeFile)
    val provinceMapcodeDF = tmpProvinceMapcodeDF.map(x=>x.getString(0).split("\t",5)).map(x=>(x(0),x(1))).distinct().toDF("provincecode", "provincename")
    val DomainAndCompanyDF = tmpDomainAndCompanyDF.join(provinceMapcodeDF,
      tmpDomainAndCompanyDF.col("belo_prov")===provinceMapcodeDF.col("provincecode")).
    select("companycode", "vpdndomain","provincecode","provincename")

    val domainAndCompanyTable = "domainAndCompanyTable_" + dataDayid
    DomainAndCompanyDF.registerTempTable(domainAndCompanyTable)

    sqlContext.sql(
      s"""select provincecode,provincename,companycode,vpdndomain
         |from
         |(
         |    select provincecode,provincename,companycode,vpdndomain,
         |           row_number() over(partition by companycode order by provincecode desc) as rn
         |    from ${domainAndCompanyTable}
         |) where rn = 1
       """.stripMargin).coalesce(1).write.format("orc").mode(SaveMode.Overwrite).save(companyAndDomainOutputPath)

    sqlContext.sql(s"alter table $companyAndDomainTable add if not exists partition(d='$dataDayid') ")

    sc.stop()
  }

}
