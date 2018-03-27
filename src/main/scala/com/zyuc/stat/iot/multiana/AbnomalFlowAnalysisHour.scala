package com.zyuc.stat.iot.multiana

import com.zyuc.stat.properties.ConfigProperties
import com.zyuc.stat.utils.{DateUtils, FileUtils}
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}


/**
  * Created by dell on 2017/8/19.
  */
object AbnomalFlowAnalysisHour {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    sqlContext.sql("use " + ConfigProperties.IOT_HIVE_DATABASE)
    val appName = sc.getConf.get("spark.app.name")
    val userTablePartitionID = sc.getConf.get("spark.app.userTablePartitionID")
    val userTable = sc.getConf.get("spark.app.table.userTable" )//"iot_customer_userinfo"
    val pdsnTable = sc.getConf.get("spark.app.table.pdsn.h") //"iot_cdr_data_pdsn_h" 2/3g
    val pgwTable = sc.getConf.get("spark.app.table.pgw.h") //"iot_cdr_data_pgw_h" 4g
    val hourid = sc.getConf.get("spark.app.hourid")
    val outputPath = sc.getConf.get("spark.app.outputPath") // hdfs://EPC-IOT-ES-06:8020/hadoop/IOT/data/multiAna/flow/
    val localOutputPath =  sc.getConf.get("spark.app.localOutputPath") // /slview/test/limm/multiAna/flow/hour/json/


    val summarytime = DateUtils.timeCalcWithFormatConvertSafe(hourid, "yyyyMMddHH", (-1)*60*60, "yyyyMMddHH")

    val partitionD = summarytime.substring(2, 8)
    val partitionH = summarytime.substring(8,10)
    val dayid      = summarytime.substring(0, 8)
    val sumhour = hourid.substring(0,10)
    val timeid = sumhour
    // company province nettype
    val tmpCompanyTable = s"${appName}_tmp_Company"
    sqlContext.sql(
      s"""select distinct (case when length(belo_prov)=0 or belo_prov is null then '其他' else belo_prov end)  as custprovince,
         |       case when length(companycode)=0 or companycode is null then 'P999999999' else companycode end  as vpdncompanycode
         |from ${userTable}
         |where d='${userTablePartitionID}'
       """.stripMargin
    ).cache().registerTempTable(tmpCompanyTable)


    val tmpCompanyNetTable = s"${appName}_tmp_CompanyNet"
    val companyDF = sqlContext.sql(
      s"""select custprovince, vpdncompanycode, '2/3G' as nettype from ${tmpCompanyTable}
         |union all
         |select custprovince, vpdncompanycode, '4G' as nettype from ${tmpCompanyTable}
       """.stripMargin
    ).cache().registerTempTable(tmpCompanyNetTable)
    //val companyName = sqlContext.table("iot_basic_company")
    //val companyDF_r = companyDF.join(companyName,companyName.col("companycode")===companyDF.col("vpdncompanycode")).select(
    //  companyDF.col("custprovince"),companyDF.col("vpdncompanycode"),companyDF.col("nettype"),companyName.col("companyname")
    //)
    // 2/3g flow
    val tmpcdrtable="tmpcdrtable"
    val flowDF = sqlContext.sql(
      s"""select vpdncompanycode,custprovince,d,h,nettype,sumupflow,sumdownflow,usernum,
         |      case when usernum = 0 then 0 else round(sumupflow/usernum,0) end avgupflow,
         |      case when usernum = 0 then 0 else round(sumdownflow/usernum,0) end avgdownflow
         |from (select vpdncompanycode,custprovince,d,h,sum(upflow) as sumupflow,sum(downflow) as sumdownflow,count(distinct mdn) as usernum,
         |           "2/3G" as nettype
         |     from  ${pdsnTable}
         |     where  d = "${partitionD}" and h ="${partitionH}"
         |     group by vpdncompanycode,custprovince,d,h,"2/3G"
         |     union all
         |     select vpdncompanycode,custprovince,d,h,sum(upflow) as sumupflow,sum(downflow) as sumdownflow,count(distinct mdn) as usernum,
         |           "4G" as nettype
         |     from  ${pgwTable}
         |     where  d = "${partitionD}" and h ="${partitionH}"
         |     group by vpdncompanycode,custprovince,d,h,"4G")t
       """.stripMargin
    ).registerTempTable(tmpcdrtable)

    val resultDF = sqlContext.sql(
      s"""select n.custprovince,n.nettype,n.vpdncompanycode as companycode,
         |      nvl(c.usernum,0) as usernum,
         |      nvl(c.sumupflow,0.0) as upflow,
         |      case when c.sumdownflow is null then 0.0 else c.sumdownflow end as downflow,
         |      case when c.avgupflow is null then 0 else c.avgupflow end as avgupflow,
         |      case when c.avgdownflow is null then 0 else c.avgdownflow end as avgdownflow,
         |      ${sumhour} as datetime
         |from  ${tmpCompanyNetTable} n
         |left join ${tmpcdrtable} c
         |on   c.vpdncompanycode = n.vpdncompanycode
         |and  c.nettype = n.nettype
       """.stripMargin

    )


    // 4g flow
  //  val pgwDF = sqlContext.sql(
  //    s"""select vpdncompanycode,custprovince,d,h,sum(upflow) as sumupflow,sum(downflow) as sumdownflow,count(distinct mdn) as usernum,
  //       |      "4G" as nettype
  //       |from  ${pgwTable}
  //       |where  d = "${partitionD}" and h ="${partitionH}"
  //       |group by vpdncompanycode,custprovince,d,h,"4G"
  //     """.stripMargin
  //  )


    //val resultDF = companyDF.join(flowDF,Seq("vpdncompanycode","nettype"),"left").
    //  select(companyDF.col("custprovince"),companyDF.col("nettype"),companyDF.col("vpdncompanycode").alias(("companycode")),
    //    when(flowDF.col("usernum").isNull,0).otherwise(flowDF.col("usernum")).alias("usernum"),
    //    when(flowDF.col("avgupflow").isNull,0).otherwise(flowDF.col("avgupflow")).alias("avgupflow"),
    //    when(flowDF.col("avgdownflow").isNull,0).otherwise(flowDF.col("avgdownflow")).alias("avgdownflow"),
    //    when(flowDF.col("sumupflow").isNull,0).otherwise(flowDF.col("sumupflow")).alias("upflow"),
    //    when(flowDF.col("sumdownflow").isNull,0).otherwise(flowDF.col("sumdownflow")).alias("downflow")
    //  ).withColumn("datetime", lit(hourid))



    val dayidreal = timeid.substring(0,8)
    val coalesceNum = 1
    val outputLocatoin = outputPath  + "tmp/" + timeid + "/"

    val fileSystem = FileSystem.newInstance(sc.hadoopConfiguration)

    resultDF.repartition(coalesceNum.toInt).write.mode(SaveMode.Overwrite).format("json").save(outputLocatoin)

    FileUtils.moveTempFilesToESpath(fileSystem,outputPath,timeid,dayidreal)
    //FileUtils.downFilesToLocal(fileSystem, outputLocatoin, localOutputPath + "/"+ hourid.substring(0,8) + "/", hourid.substring(8,10), ".json")

    sc.stop()
  }

}





