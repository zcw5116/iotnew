package com.zyuc.stat.iot.multiana

import com.zyuc.stat.properties.ConfigProperties
import com.zyuc.stat.utils.{DateUtils, FileUtils}
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.SaveMode
import org.apache.spark.{Logging, SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.functions.lit

/**
  * Created by dell on 2017/11/29.
  * 异常流量和活跃用户数按照天/小时汇总
  * 流量为所有卡流量之和
  * 活跃数为所有去重卡总数
  */
object CdrActiveAndFlowAnalysis extends Logging{
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    sqlContext.sql("use " + ConfigProperties.IOT_HIVE_DATABASE)

    val appName = sc.getConf.get("spark.app.name","flow_and_active_analysis")
    val userTablePartitionID = sc.getConf.get("spark.app.userTablePartitionID","20170922")
    val userTable = sc.getConf.get("spark.app.table.userTable","iot_basic_userinfo" )//"iot_customer_userinfo"
    val pdsnTable = sc.getConf.get("spark.app.table.pdsn","iot_cdr_etl_pdsn_h") //"iot_cdr_data_pdsn_h" 2/3g
    val pgwTable = sc.getConf.get("spark.app.table.pgw","iot_cdr_etl_pgw_h") //"iot_cdr_data_pgw_h" 4g
    val haccgTable = sc.getConf.get("spark.app.table.haccg","iot_cdr_etl_haccg_h") //"iot_cdr_data_haccg_h" vpdn 3g
    val timeType = sc.getConf.get("spark.app.timeType","h") //h,d
    val timeid = sc.getConf.get("spark.app.timeid") //2017113021
    val outputPath = sc.getConf.get("spark.app.outputPath","hdfs://EPC-IOT-ES-06:8020/hadoop/IOT/data/multiAnaly/flow/")
    val localOutputPath =  sc.getConf.get("spark.app.localOutputPath") // /slview/test/limm/multiAna/flow/hour/json/
    val vpnToApnMapFile = sc.getConf.get("spark.app.vpnToApnMapFile", "/hadoop/IOT/ANALY_PLATFORM/BasicData/VpdnToApn/vpdntoapn.txt")

    import sqlContext.implicits._

    val vpnToApnDF  = sqlContext.read.format("text").load(vpnToApnMapFile).map(x=>x.getString(0).split(",")).map(x=>(x(0),x(1))).toDF("vpdndomain","apn")
    val vpdnAndApnTable = "vpdnAndApnTable"
    vpnToApnDF.registerTempTable(vpdnAndApnTable)

    var datetime:String = null
    var partitionH:String = null
    var partitionD:String = null
    var tmptime:String = null
    val hourid:String = timeid.substring(0,10)
    // 获取话单数据分区
    if (timeType == "h"){
      datetime =hourid
      tmptime= DateUtils.timeCalcWithFormatConvertSafe(hourid, "yyyyMMddHH", (-1)*60*60, "yyyyMMddHH")
      partitionH = tmptime.substring(8,10)
      partitionD = tmptime.substring(2,8)
    }else if(timeType == "d"){
      datetime = timeid.substring(0,8)
      partitionD = timeid.substring(2,8)
    }

    logInfo("##########--pdsnTable: " + pdsnTable)
    logInfo("##########--pgwTable: " + pgwTable)
    logInfo("##########--haccgTable: " + haccgTable)
    logInfo("##########--timeType: " + timeType)
    logInfo("##########--timeid: " + timeid)
    logInfo("##########--datetime: " + datetime)
    logInfo("##########--partitionD: " + partitionD)
    logInfo("##########--partitionH: " + partitionH)
    logInfo("##########--outputPath: " + outputPath)
    logInfo("##########--userTablePartitionID: " + userTablePartitionID)



    val companyTabName = "tmpCompanyInfo"
    //val companyCol = sqlContext.sql("select companycode ,provincecode from iot_basic_company_and_domain ").collect()
    //val companyBd =  sc.broadcast(companyCol)
    //val companyValue = companyBd.value
    //import sqlContext.implicits._
    //sc.parallelize(companyValue).map(x=>(x(0).toString, x(1).toString)).toDF("companycode", "provincecode").registerTempTable(companyTabName)

    sqlContext.sql("select companycode ,provincecode from iot_basic_company_and_domain ").registerTempTable(companyTabName)
    //企业表与用户表关联，去除用户表中省份为空的企业
    val rstUserTab = "rstUserTab"
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

    //关联cdr获取流量与活跃用户
    var mdnSql:String = null
    if (timeType == "h"){
       mdnSql =
        s"""
           |select provincecode,companycode,type, mdn, servtype, mdndomain as vpdndomain, bsid, downflow, upflow
           |from
           |(
           |select u.provincecode,u.companycode, '2/3G' type, a.mdn,
           |       'C' as servtype,
           |       (case when array_contains(split(vpdndomain,','), regexp_replace(nai,'.*@','')) then regexp_replace(nai,'.*@','') else vpdndomain end) as mdndomains,
           |       a.bsid, a.termination as downflow, a.originating as upflow
           |from  ${pdsnTable} a, ${rstUserTab} u
           |where a.d = '${partitionD}'  and a.h = '${partitionH}'
           |      and a.source_ip_address='0.0.0.0'
           |      and a.mdn = u.mdn and u.isvpdn='1'
           |) t lateral view explode(split(t.mdndomains,',')) c as mdndomain
           |union all
           |select provincecode,u.companycode, '2/3G' type, a.mdn,
           |       (case when u.isdirect=1 then 'D' else 'P' end) as servtype,
           |       '-1'  as vpdndomain,
           |       a.bsid, a.termination as downflow, a.originating as upflow
           |from ${haccgTable} a, ${rstUserTab} u
           |where a.d = '${partitionD}'  and a.h = '${partitionH}'
           |      and u.mdn = a.mdn
           |union all
           |select u.provincecode,u.companycode,'4G' type,u.mdn,
           |(case when u.isdirect='1' then 'D' when u.isvpdn=1 and array_contains(split(u.vpdndomain,','), d.vpdndomain) then 'C' else 'P' end) as servtype,
           |(case when u.isvpdn ='1' and array_contains(split(u.vpdndomain,','), d.vpdndomain) then d.vpdndomain else '-1' end)  as vpdndomain,
           |u.bsid, downflow, upflow
           |from
           |(
           |    select t.provincecode,p.mdn,p.accesspointnameni as apn, t.companycode, t.vpdndomain, t.isdirect, t.isvpdn,p.bsid,
           |           nvl(downflow,0) as downflow, nvl(upflow,0) as upflow
           |    from ${pgwTable} p, ${rstUserTab} t
           |    where p.mdn = t.mdn and p.d = '${partitionD}'  and p.h = '${partitionH}'
           |) u
           |left join ${vpdnAndApnTable} d
           |
           |on(u.apn = d.apn and array_contains(split(u.vpdndomain,','),d.vpdndomain) )
       """.stripMargin
    }else if(timeType == "d"){
      mdnSql =
        s"""
           select provincecode,companycode, type, mdn, servtype, mdndomain as vpdndomain, bsid, downflow, upflow
           |from
           |(
           |select u.provincecode,u.companycode, '2/3G' as type, a.mdn,
           |       'C' as servtype,
           |       (case when array_contains(split(vpdndomain,','), regexp_replace(nai,'.*@','')) then regexp_replace(nai,'.*@','') else vpdndomain end) as mdndomains,
           |       a.bsid, a.termination as downflow, a.originating as upflow
           |from  ${pdsnTable} a, ${rstUserTab} u
           |where a.d = '${partitionD}'
           |      and a.source_ip_address='0.0.0.0'
           |      and a.mdn = u.mdn and u.isvpdn='1'
           |) t lateral view explode(split(t.mdndomains,',')) c as mdndomain
           |union all
           |select provincecode,u.companycode, '2/3G' as type, a.mdn,
           |       (case when u.isdirect=1 then 'D' else 'P' end) as servtype,
           |       '-1'  as vpdndomain,
           |       a.bsid, a.termination as downflow, a.originating as upflow
           |from ${haccgTable} a, ${rstUserTab} u
           |where a.d = '${partitionD}'
           |      and u.mdn = a.mdn
           |union all
           |select u.provincecode,u.companycode, '4G' as type,u.mdn,
           |(case when u.isdirect='1' then 'D' when u.isvpdn=1 and array_contains(split(u.vpdndomain,','), d.vpdndomain) then 'C' else 'P' end) as servtype,
           |(case when u.isvpdn ='1' and array_contains(split(u.vpdndomain,','), d.vpdndomain) then d.vpdndomain else '-1' end)  as vpdndomain,
           |u.bsid, downflow, upflow
           |from
           |(
           |    select t.provincecode,p.mdn,p.accesspointnameni as apn, t.companycode, t.vpdndomain, t.isdirect, t.isvpdn,p.bsid,
           |           nvl(downflow,0) as downflow, nvl(upflow,0) as upflow
           |    from ${pgwTable} p, ${rstUserTab} t
           |    where p.mdn = t.mdn and p.d = '${partitionD}'
           |) u
           |left join ${vpdnAndApnTable} d
           |on(u.apn = d.apn and array_contains(split(u.vpdndomain,','),d.vpdndomain) )
       """.stripMargin
    }



    val mdnTable = "mdnTable_" + datetime
    sqlContext.sql(mdnSql).registerTempTable(mdnTable)

    //sqlContext.cacheTable(mdnTable)
    logInfo("##########--begin agg... " )
    //val aggDF =
    //  sqlContext.sql( s"""
    //     |select provincecode ,companycode  , servtype, vpdndomain,
    //     |       type,
    //     |       sum(upflow) as upflow,
    //     |       sum(downflow) as downflow,
    //     |       avg(upflow) as avgupflow,
    //     |       avg(downflow) as avgdownflow,
    //     |       count(distinct mdn) as usernum
    //     |from ${mdnTable}  m
    //     |group by provincecode,companycode, type, servtype, vpdndomain
    //   """.stripMargin)

    val aggDF =
      sqlContext.sql( s"""
                         |select provincecode ,companycode  , servtype, vpdndomain,
                         |       type,
                         |       sum(upflow) as upflow,
                         |       sum(downflow) as downflow,
                         |       avg(upflow) as avgupflow,
                         |       avg(downflow) as avgdownflow,
                         |       sum(0) as usernum
                         |from ${mdnTable}  m
                         |group by provincecode,companycode, type, servtype, vpdndomain
       """.stripMargin)


    aggDF.show(2)
    // servtype  vpdndomain
    val resultDF = aggDF.selectExpr("case when length(companycode) = 0 or companycode is null " +
      "then 'N999999999' else companycode end as companycode" ,
      "provincecode as custprovince" ,
      "servtype","type as nettype" ,"vpdndomain as domain" ,
      "nvl(upflow,0) as upflow","nvl(downflow,0) as downflow","nvl(round(avgupflow,0),0) as avgupflow","nvl(round(avgdownflow,0),0) as avgdownflow",
      "nvl(usernum,0) as usernum").
      withColumn("datetime", lit(datetime))

    resultDF.show(3)
    val pathdayid = timeid.substring(0,8)
    val coalesceNum = 1
    val outputLocatoin = outputPath  + "tmp/" + datetime + "/"

    val fileSystem = FileSystem.newInstance(sc.hadoopConfiguration)

    resultDF.repartition(coalesceNum.toInt).write.mode(SaveMode.Overwrite).format("json").save(outputLocatoin)

    FileUtils.moveTempFilesToESpath(fileSystem,outputPath,timeid,pathdayid)
    //FileUtils.downFilesToLocal(fileSystem, outputLocatoin, localOutputPath + "/"+ hourid.substring(0,8) + "/", hourid.substring(8,10), ".json")

    sc.stop()




  }

}
