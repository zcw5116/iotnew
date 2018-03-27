package com.zyuc.stat.iot.multiana

import com.zyuc.stat.properties.ConfigProperties
import com.zyuc.stat.utils.FileUtils
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{Logging, SparkConf, SparkContext}
import org.apache.spark.sql.functions._
/**
  * Created by LiJian on 2017/7/28.
  */
object IotTerminalAnaly extends Logging{
  def main(args: Array[String]) {
    val sparkConf = new SparkConf()//.setMaster("local[4]").setAppName("fd")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    //sqlContext.sql("use iot" )
    import sqlContext.implicits._
    sqlContext.sql("use " + ConfigProperties.IOT_HIVE_DATABASE)
    val userTable = sc.getConf.get("spark.app.user.tableName","iot_basic_userinfo")     // "iot_basic_userinfo"
    val userTablePartitionDayid:String = sc.getConf.get("spark.app.user.userTablePartitionDayid","20170922")  //  "20170922"
    val partiondayid = sc.getConf.get("spark.app.user.dayid",userTablePartitionDayid)  //  "20170922"
    val terminalInputPath = sc.getConf.get("spark.app.terminalInputPath","")  // /hadoop/IOT/data/terminal/output/data/  iot_dim_terminal
    val terminalOutputPath = sc.getConf.get("spark.app.terminalOutputPath","")  // /hadoop/IOT/data/terminal/output/
    val appName = sc.getConf.get("spark.app.name","terminal_analy") // terminalmultiAnalysis_2017073111
    val coalesceNum = sc.getConf.get("spark.app.coalesceNum", "1")

    //val localOutputPath = sc.getConf.get("spark.app.localOutputPath","/slview/test/zcw/shell/terminal/json/")  // /slview/test/zcw/shell/terminal/json/
    //val terminalResultTable = sc.getConf.get("spark.app.terminalResultTable")
    //val companyInfoTable = sc.getConf.get("spark.app.table.companyInfo") //"iot_activeduser_data_day"

    val fileSystem = FileSystem.get(sc.hadoopConfiguration)

    val terminalLocation = terminalInputPath + "/data"
    val terminalTab = "terminalTab"
    sqlContext.read.format("orc").load(terminalLocation).registerTempTable(terminalTab)



    logInfo("##########--partiondayid:"+partiondayid)
    logInfo("##########--userTablePartitionDayid:"+userTablePartitionDayid)


    val companyTabName = "tmpCompanyInfo"
    val companyCol = sqlContext.sql("select companycode ,provincecode from iot_basic_company_and_domain ").collect()
    val companyBd =  sc.broadcast(companyCol)
    val companyValue = companyBd.value
    //import sqlContext.implicits._
    sc.parallelize(companyValue).map(x=>(x(0).toString, x(1).toString)).toDF("companycode", "provincecode").registerTempTable(companyTabName)

    val tmpResultTab = "tmpResultTab"
    //对mdn?imei计数?卡去重
    //
    sqlContext.sql(
      s"""CACHE TABLE ${tmpResultTab} as
         |select f.mdn,f.imei,t.devicetype ,t.modelname,f.provincecode,f.companycode,f.servtype,f.domain
         | from (
         |        select u.mdn,u.imei,
         |               case when u.isvpdn = '1' then 'C' when  u.isdirect = '1' then 'D' else 'P' end as servtype,
         |               case when u.isvpdn = '1' then u.vpdndomain else '-1' end as domain,
         |               case when length(c.provincecode)=0 or c.provincecode is null then 'N999999999' else c.provincecode end  as provincecode,
         |               case when length(u.companycode)=0 then 'P999999999' else u.companycode end  as companycode
         |        from   ${userTable} u
         |        left   join ${companyTabName} c
         |        on     c.companycode = u.companycode
         |        where  u.d = ${userTablePartitionDayid} ) f
         |left join ${terminalTab} t
         |on substr(f.imei,1,8) = t.tac
       """.stripMargin)
      //.registerTempTable(tmpResultTab)


    //汇总业务按照企业，公司，业务类型，域名
    val aggDF =
      sqlContext.sql(
      s"""select  devicetype,modelname,provincecode,companycode,servtype ,'-1' as domain,count(*) as tercnt
         |from  ${tmpResultTab}
         |group by devicetype,modelname,provincecode,companycode,servtype,'-1'
         |union all
         |select devicetype,modelname,provincecode,companycode,servtype ,c.domain,count(*) as tercnt
         |from   ${tmpResultTab} t lateral view explode(split(t.domain,',')) c as domain
         |where  servtype = 'C'
         |group by devicetype,modelname,provincecode,companycode,servtype,c.domain
       """.stripMargin).cache()


    val resultDF = aggDF.select(aggDF.col("devicetype"),aggDF.col("modelname"),aggDF.col("provincecode").alias("custprovince"),aggDF.col("companycode"),
      aggDF.col("servtype"),aggDF.col("domain"),when((aggDF.col("tercnt")).isNull,0).otherwise(aggDF.col("tercnt")).alias("tercnt")).
      withColumn("datetime",lit(partiondayid))
      //val userDF = sqlContext.table(userTable).filter("d=" + userTablePartitionDayid).
    // selectExpr("mdn", "imei", "case when length(belo_prov)=0 or belo_prov is null then '其他' else belo_prov end  as custprovince",
      //  "case when length(companycode)=0 then 'P999999999' else companycode end  as vpdncompanycode").
      //cache()

    //val aggTmpDF = userDF.join(terminalDF, userDF.col("imei").substr(0,8)===terminalDF.col("tac"), "left").
    //  groupBy(userDF.col("custprovince"), userDF.col("vpdncompanycode"), terminalDF.col("devicetype"),
    //    terminalDF.col("modelname")).agg(count(lit(1)).alias("tercnt"))
//
//
    //val  aggDF = aggTmpDF
//
//
    // 处理空值
    //val resultDF = aggDF.select(lit(partiondayid).alias("datetime"),
    //  when(aggDF.col("custprovince").isNull, "其他").otherwise(aggDF.col("custprovince")).alias("custprovince"),
    //  when(aggDF.col("vpdncompanycode").isNull, "P999999999").otherwise(aggDF.col("vpdncompanycode")).alias("companycode"),
    //  when(aggDF.col("devicetype").isNull, "").otherwise(aggDF.col("devicetype")).alias("devicetype"),
    //  when(aggDF.col("modelname").isNull, "").otherwise(aggDF.col("modelname")).alias("modelname"),
    //  aggDF.col("tercnt"))
////
//
    val terminalOutputLocatoin = terminalOutputPath  + "tmp/" + partiondayid+"/"
    val timeid = partiondayid
    resultDF.repartition(coalesceNum.toInt).write.mode(SaveMode.Overwrite).format("json").save(terminalOutputLocatoin)
    FileUtils.moveTempFilesToESpath(fileSystem,terminalOutputPath,partiondayid,timeid)
    //FileUtils.downFilesToLocal(fileSystem, terminalOutputLocatoin, localOutputPath, partiondayid, ".json")

  }
}
