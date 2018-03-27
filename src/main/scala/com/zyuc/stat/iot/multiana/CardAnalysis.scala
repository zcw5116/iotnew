package com.zyuc.stat.iot.multiana


import com.zyuc.stat.properties.ConfigProperties
import com.zyuc.stat.utils.FileUtils
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{Logging, SparkConf, SparkContext}
import org.apache.spark.sql.functions._


/**
  * Created by zhoucw on 17-8-17.
  * modify by limm 20171129
  *
  */
  object CardAnalysis extends Logging {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()//.setAppName("OperalogAnalysis").setMaster("local[4]")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    sqlContext.sql("use " + ConfigProperties.IOT_HIVE_DATABASE)

    val appName =  sc.getConf.get("spark.app.name")
    val userTablePartitionID = sc.getConf.get("spark.app.userTablePartitionID")
    val userTable = sc.getConf.get("spark.app.table.userTable","iot_basic_userinfo") //"iot_customer_userinfo" => 'iot_basic_userinfo'
    val operTable = sc.getConf.get("spark.app.table.operaLogTable") //"iot_operlog_data_day"
    val onlineTable = sc.getConf.get("spark.app.table.onlineTable","iot_useronline_basedata")
    val activedUserPath = sc.getConf.get("spark.app.table.activePath") //"iot_activeduser_data_day"
    //val companyInfoTable = sc.getConf.get("spark.app.table.companyInfo") //"iot_activeduser_data_day"
    val dayid = sc.getConf.get("spark.app.dayid")
    val outputPath = sc.getConf.get("spark.app.outputPath") // hdfs://EPC-IOT-ES-06:8020/hadoop/IOT/data/multiAna/card/
    //val localOutputPath =  sc.getConf.get("spark.app.localOutputPath")
    // /slview/test/zcw/shell/card/json/
    val partitionD = dayid.substring(2, 8)
    val beginHour = partitionD + "00"
    val endHour = partitionD + "23"

    val companyTabName = "tmpCompanyInfo"
    val companyCol = sqlContext.sql("select case when length(companycode)=0 or companycode is null then 'P999999999' else companycode end as companycode ," +
      "case when length(provincecode)=0 or provincecode is null then 'N999999999' else provincecode end as provincecode from iot_basic_company_and_domain ").collect()
    val companyBd =  sc.broadcast(companyCol)
    val companyValue = companyBd.value
    import sqlContext.implicits._
    sc.parallelize(companyValue).map(x=>(x(0).toString, x(1).toString)).toDF("companycode", "provincecode").registerTempTable(companyTabName)

    //企业表与用户表关联，去除用户表中省份为空的企业
    val rstUserTab = "rstUserTab"
    logInfo("##########--begin uses... " )
    sqlContext.sql(
      s"""   CACHE TABLE ${rstUserTab} as
         |        select u.mdn,u.imei,u.isvpdn,u.isdirect ,u.vpdndomain,u.iscommon,
         |               case when length(c.provincecode)=0 or c.provincecode is null then 'N999999999' else c.provincecode end  as provincecode,
         |               case when length(u.companycode)=0 then 'P999999999' else u.companycode end  as companycode
         |        from   ${userTable} u
         |        left join ${companyTabName} c
         |        on c.companycode = u.companycode
         |        where  d = '${userTablePartitionID}'
       """.stripMargin)
      //.registerTempTable(rstUserTab)
    //companycode             string
    //  servtype                string
    //  vpdndomain              string
    //type                    string
    //usercnt                 bigint

    logInfo("##########--begin online... " )
    val onlineDF =
      sqlContext.sql(
      s"""
         |select c.provincecode,o.companycode,o.servtype,o.type,vpdndomain,floor(avg(o.usercnt)) as usercnt
         |from   ${onlineTable} o
         |left join ${companyTabName} c
         |on   o.companycode = c.companycode
         |where  o.d >= '${beginHour}' and o.d<= '${endHour}'
         |group by c.provincecode,o.companycode,o.servtype,o.type,vpdndomain
       """.stripMargin)

    var resultDF:DataFrame = onlineDF.selectExpr("case when length(provincecode)=0 or provincecode is null then 'N999999999' else provincecode end as provincecode " ,
      "case when length(companycode)=0 or companycode is null then 'P999999999' else companycode end as companycode" ,
      "servtype" ,"vpdndomain as domain" ,
      "case when type = '3g' then '2/3G' when  type = '4g' then '4G' else type end nettype" ,
      "case when usercnt is null then 0 else usercnt end onlinenum")

    //val tmpCompanyTable = s"${appName}_tmp_Company"
    //sqlContext.sql(
    //  s"""select distinct (case when length(belo_prov)=0 or belo_prov is null then '其他' else belo_prov end)  as custprovince,
    //     |           case when length(companycode)=0 or companycode is null then 'P999999999' else companycode end  as vpdncompanycode
    //     |    from ${userTable}
    //     |    where d='${userTablePartitionID}'
    //     |
    //   """.stripMargin).cache().registerTempTable(tmpCompanyTable)
//
    //val tmpCompanyNetTable = s"${appName}_tmp_CompanyNet"
    //val companyDF = sqlContext.sql(
    //  s"""select custprovince, vpdncompanycode,  '2/3G' as nettype from ${tmpCompanyTable}
    //     |union all
    //     |select custprovince, vpdncompanycode,  '4G' as nettype from ${tmpCompanyTable}
    //     |union all
    //     |select custprovince, vpdncompanycode,  '2/3/4G' as nettype from ${tmpCompanyTable}
    //   """.stripMargin
    //).cache()
//
//
    //// Operlog
    //val operDF = sqlContext.table(operTable).filter("d="+partitionD)
    //var resultDF = companyDF.join(operDF, companyDF.col("vpdncompanycode")===operDF.col("vpdncompanycode") &&
    //  companyDF.col("nettype")===operDF.col("nettype"), "left").select(companyDF.col("custprovince"),
    //  companyDF.col("vpdncompanycode"),  companyDF.col("nettype"),
    //  operDF.col("opennum"),  operDF.col("closenum"))
//
    //
    //val onlineHourDF = sqlContext.sql(
    //  s"""select vpdncompanycode,floor(avg(g3cnt)) onlinenum,'2/3G' as nettype
    //     |from  ${onlineTable}
    //     |where d = "${partitionD}"
    //     |group by vpdncompanycode,'2/3G'
    //     |union all
    //     |select vpdncompanycode,floor(avg(pgwcnt)) onlinenum,'4G' as nettype
    //     |from  ${onlineTable}
    //     |where d = "${partitionD}"
    //     |group by vpdncompanycode,'4G'
    //   """.stripMargin
    //)
    //
//
    //resultDF =  resultDF.join(onlineHourDF, resultDF.col("vpdncompanycode")===onlineHourDF.col("vpdncompanycode") &&
    //  resultDF.col("nettype")===onlineHourDF.col("nettype"), "left").select(resultDF.col("custprovince"),
    //  resultDF.col("vpdncompanycode"),  resultDF.col("nettype"), resultDF.col("opennum"),
    //  resultDF.col("closenum"),onlineHourDF.col("onlinenum"))
//
    logInfo("##########--begin active... " )

    // active
    val activedUserTable = "tmpActiveTab"

    sqlContext.read.format("json").load(activedUserPath).registerTempTable(activedUserTable)
    val activedUserDF = sqlContext.table(activedUserTable).selectExpr("provincecode","companycode",
      "nettype","servtype","domain", "usernum")

    resultDF =  resultDF.join(activedUserDF, resultDF.col("companycode")===activedUserDF.col("companycode") &&
      resultDF.col("nettype")===activedUserDF.col("nettype") &&
       resultDF.col("servtype")===activedUserDF.col("servtype") &&
      resultDF.col("domain")===activedUserDF.col("domain"), "left").
      select(
      resultDF.col("provincecode").alias("custprovince"), resultDF.col("companycode"),
        resultDF.col("nettype"),resultDF.col("servtype"),resultDF.col("domain"),
        when(resultDF.col("onlinenum").isNull,0).otherwise(resultDF.col("onlinenum")).alias("onlinenum"),
        when(activedUserDF.col("usernum").isNull,0).otherwise(activedUserDF.col("usernum")).alias("activednum")).
      withColumn("datetime", lit(dayid)).withColumn("opennum",lit(0)).withColumn("closenum",lit(0))
//

    val coalesceNum = 1
    val outputLocatoin = outputPath + "tmp/" + dayid + "/"
    val fileSystem = FileSystem.newInstance(sc.hadoopConfiguration)

    resultDF.repartition(coalesceNum.toInt).write.mode(SaveMode.Overwrite).format("json").save(outputLocatoin)
    FileUtils.moveTempFilesToESpath(fileSystem,outputPath,dayid,dayid)
    //FileUtils.downFilesToLocal(fileSystem, outputLocatoin, localOutputPath, dayid, ".json")

  }



}
