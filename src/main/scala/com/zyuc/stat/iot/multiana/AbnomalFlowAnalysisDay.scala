package com.zyuc.stat.iot.multiana

import com.zyuc.stat.properties.ConfigProperties
import com.zyuc.stat.utils.FileUtils
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by dell on 2017/8/27.
  */
object AbnomalFlowAnalysisDay {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    sqlContext.sql("use " + ConfigProperties.IOT_HIVE_DATABASE)
    val appName = sc.getConf.get("spark.app.name")
    val userTablePartitionID = sc.getConf.get("spark.app.userTablePartitionID")
    val userTable = sc.getConf.get("spark.app.table.userTable" )//"iot_customer_userinfo"
    val pdsnTable = sc.getConf.get("spark.app.table.pdsn.d") //"iot_cdr_data_pdsn_d" 2/3g
    val pgwTable = sc.getConf.get("spark.app.table.pgw.d") //"iot_cdr_data_pgw_d" 4g
    val dayid = sc.getConf.get("spark.app.dayid")
    val outputPath = sc.getConf.get("spark.app.outputPath") // hdfs://EPC-IOT-ES-06:8020/hadoop/IOT/data/multiAna/flow/
    val localOutputPath =  sc.getConf.get("spark.app.localOutputPath") // /slview/test/limm/multiAna/flow/hour/json/
    val partitionD = dayid.substring(2)


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



    val tmpcdrtable="tmpcdrtable"
    val flowDF = sqlContext.sql(
      s"""select vpdncompanycode,custprovince,d,nettype,sumupflow,sumdownflow,usernum,
         |      case when usernum = 0 then 0 else round(sumupflow/usernum,0) end avgupflow,
         |      case when usernum = 0 then 0 else round(sumdownflow/usernum,0) end avgdownflow
         |from (select vpdncompanycode,custprovince,d,sum(upflow) as sumupflow,sum(downflow) as sumdownflow,count(distinct mdn) as usernum,
         |           "2/3G" as nettype
         |     from  ${pdsnTable}
         |     where  d = "${partitionD}"
         |     group by vpdncompanycode,custprovince,d,"2/3G"
         |     union all
         |     select vpdncompanycode,custprovince,d,sum(upflow) as sumupflow,sum(downflow) as sumdownflow,count(distinct mdn) as usernum,
         |           "4G" as nettype
         |     from  ${pgwTable}
         |     where  d = "${partitionD}"
         |     group by vpdncompanycode,custprovince,d,"4G")t
       """.stripMargin
    ).registerTempTable(tmpcdrtable)


    val resultDF = sqlContext.sql(
      s"""select n.custprovince,n.nettype,n.vpdncompanycode as companycode,
         |      nvl(c.usernum,0) as usernum,
         |      nvl(c.sumupflow,0.0) as upflow,
         |      nvl(c.sumdownflow,0.0) as downflow,
         |      nvl(c.avgupflow,0.0) as avgupflow,
         |      nvl(c.avgdownflow,0.0) as avgdownflow,
         |      ${dayid} as datetime
         |from  ${tmpCompanyNetTable} n
         |left join ${tmpcdrtable} c
         |on   c.vpdncompanycode = n.vpdncompanycode
         |and  c.nettype = n.nettype
       """.stripMargin
    )



    // 2/3g flow
    //val pdsnDF = sqlContext.sql(
    //  s"""select vpdncompanycode,custprovince,d,sum(upflow) as sumupflow,sum(downflow) as sumdownflow,count(distinct mdn) as usernum,
    //     |      "2/3G" as nettype
    //     |from  ${pdsnTable}
    //     |where  d = "${partitionD}"
    //     |group by vpdncompanycode,custprovince,d,"2/3G"
    //   """.stripMargin
    //)
//
    //// 4g flow
    //val pgwDF = sqlContext.sql(
    //  s"""select vpdncompanycode,custprovince,d,sum(upflqow) as sumupflow,sum(downflow) as sumdownflow,count(distinct mdn) as usernum,
    //     |      "4G" as nettype
    //     |from  ${pgwTable}
    //     |where  d = "${partitionD}"
    //     |group by vpdncompanycode,custprovince,d,"4G"
    //   """.stripMargin
    //)

   // val flowDF = pdsnDF.select(pdsnDF.col("vpdncompanycode"),pdsnDF.col("custprovince"),pdsnDF.col("d"),pdsnDF.col("h"),pdsnDF.col("sumupflow"),
   //   pdsnDF.col("sumdownflow"),pdsnDF.col("usernum"),
   //   when(pdsnDF.col("usernum")===0, 0).otherwise((pdsnDF.col("sumupflow")/pdsnDF.col("usernum"))).alias("avgupflow"),
   //   when(pdsnDF.col("usernum")===0, 0).otherwise((pdsnDF.col("sumdownflow")/pdsnDF.col("usernum"))).alias("avgdownflow"),
   //   pdsnDF.col("nettype")
   // ).unionAll(
   //   pgwDF.select(pgwDF.col("vpdncompanycode"),pgwDF.col("custprovince"),pgwDF.col("d"),pgwDF.col("h"),pgwDF.col("sumupflow"),
   //     pgwDF.col("sumdownflow"),pgwDF.col("usernum"),
   //     when(pgwDF.col("usernum")===0, 0).otherwise((pgwDF.col("sumupflow")/pgwDF.col("usernum"))).alias("avgupflow"),
   //     when(pgwDF.col("usernum")===0, 0).otherwise((pgwDF.col("sumdownflow")/pgwDF.col("usernum"))).alias("avgdownflow"),
   //     pgwDF.col("nettype")
//
   //   )
   // )


   //val resultDF = companyDF_r.join(flowDF,Seq("vpdncompanycode","nettype"),"left").
   //  select(companyDF_r.col("custprovince"),companyDF_r.col("nettype"),companyDF_r.col("companyname"),
   //    when(flowDF.col("usernum").isNull,0).otherwise(flowDF.col("usernum")).alias("usernum"),
   //    when(flowDF.col("avgupflow").isNull,0).otherwise(round(flowDF.col("avgupflow"),0)).alias("avgupflow"),
   //    when(flowDF.col("avgdownflow").isNull,0).otherwise(round(flowDF.col("avgdownflow"),0)).alias("avgdownflow"),
   //    when(flowDF.col("sumupflow").isNull,0).otherwise(round(flowDF.col("sumupflow"),0)).alias("upflow"),
   //    when(flowDF.col("sumdownflow").isNull,0).otherwise(round(flowDF.col("sumdownflow"),0)).alias("downflow")
   //  ).withColumn("datetime", lit(dayid))




    val coalesceNum = 1
    val tmpoutputPath = outputPath + "tmp/" + dayid + "/"
    val daytime = dayid

    val fileSystem = FileSystem.newInstance(sc.hadoopConfiguration)

    //存储在临时目录
    resultDF.repartition(coalesceNum.toInt).write.mode(SaveMode.Overwrite).format("json").save(tmpoutputPath)
    FileUtils.moveTempFilesToESpath(fileSystem,outputPath,dayid,daytime)
    // FileUtils.downFilesToLocal(fileSystem, outputLocatoin, localOutputPath , dayid, ".json")

    sc.stop()
  }

}
