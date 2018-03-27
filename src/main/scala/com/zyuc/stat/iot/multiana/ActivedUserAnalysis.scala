package com.zyuc.stat.iot.multiana

import com.zyuc.stat.iot.etl.util.CommonETLUtils
import com.zyuc.stat.properties.ConfigProperties
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{Logging, SparkConf, SparkContext}

/**
  * Created by zhoucw on 17-8-17.
  * modify by limm 2017/09/04
  */
object ActivedUserAnalysis extends Logging{

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()//.setAppName("UserOnlineBaseData").setMaster("local[4]")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    val hivedb = ConfigProperties.IOT_HIVE_DATABASE
    sqlContext.sql("use " + hivedb)
    val appName =  sc.getConf.get("spark.app.name") //
    import sqlContext.implicits._
    val pdsnTable = sc.getConf.get("spark.app.table.pdsn")  //iot_cdr_data_pdsn_d 3g
    val pgwTable = sc.getConf.get("spark.app.table.pgw")  //iot_cdr_data_pgw_d   4g
    val haccgTable = sc.getConf.get("spark.app.table.haccg")  //iot_cdr_data_haccg_d  3g
    val userTablePartitionID = sc.getConf.get("spark.app.userTablePartitionID")
    val userTable = sc.getConf.get("spark.app.table.userTable") //"iot_customer_userinfo"
    val activeUserTable = sc.getConf.get("spark.app.table.activeUserTable","iot_cdr_analy_d")
    val dayid = sc.getConf.get("spark.app.dayid")
    val outputPath = sc.getConf.get("spark.app.outputPath")  //  "hdfs://EPC-IOT-ES-06:8020/hadoop/IOT/data/activeUser/secondaryoutput/"
    val vpnToApnMapFile = sc.getConf.get("spark.app.vpnToApnMapFile", "/hadoop/IOT/ANALY_PLATFORM/BasicData/VpdnToApn/vpdntoapn.txt")



    val vpnToApnDF  = sqlContext.read.format("text").load(vpnToApnMapFile).map(x=>x.getString(0).split(",")).map(x=>(x(0),x(1))).toDF("vpdndomain","apn")
    val vpdnAndApnTable = "vpdnAndApnTable"
    vpnToApnDF.registerTempTable(vpdnAndApnTable)


    val companyTabName = "tmpCompanyInfo"
    val companyCol = sqlContext.sql("select companycode ,provincecode from iot_basic_company_and_domain ").collect()
    val companyBd =  sc.broadcast(companyCol)
    val companyValue = companyBd.value
    //import sqlContext.implicits._
    sc.parallelize(companyValue).map(x=>(x(0).toString, x(1).toString)).toDF("companycode", "provincecode").registerTempTable(companyTabName)

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
           |where a.d = '${dayid}'
           |      and a.source_ip_address='0.0.0.0'
           |      and a.mdn = u.mdn and u.isvpdn='1'
           |) t lateral view explode(split(t.mdndomains,',')) c as mdndomain
           |union all
           |select provincecode,u.companycode,'2/3G' as type, a.mdn,
           |       (case when u.isdirect=1 then 'D' else 'P' end) as servtype,
           |       '-1'  as vpdndomain,
           |       a.bsid, a.termination as downflow, a.originating as upflow
           |from ${haccgTable} a, ${rstUserTab} u
           |where a.d = '${dayid}'
           |      and u.mdn = a.mdn
           |union all
           |select u.provincecode,u.companycode,,'4G' as  type,u.mdn,
           |(case when u.isdirect='1' then 'D' when u.isvpdn=1 and array_contains(split(u.vpdndomain,','), d.vpdndomain) then 'C' else 'P' end) as servtype,
           |(case when u.isvpdn ='1' and array_contains(split(u.vpdndomain,','), d.vpdndomain) then d.vpdndomain else '-1' end)  as vpdndomain,
           |u.bsid, downflow, upflow
           |from
           |(
           |    select t.provincecode,p.mdn,p.accesspointnameni as apn, t.companycode, t.vpdndomain, t.isdirect, t.isvpdn,p.bsid,
           |           nvl(l_datavolumefbcdownlink,0) as downflow, nvl(l_datavolumefbcuplink,0) as upflow
           |    from ${pgwTable} p, ${rstUserTab} t
           |    where p.mdn = t.mdn and p.d = '${dayid}'
           |) u
           |left join ${vpdnAndApnTable} d
           |on(u.apn = d.apn and array_contains(split(u.vpdndomain,','),d.vpdndomain) )
       """.stripMargin




    val mdnTable = "mdnTable_" + dayid
    sqlContext.sql(mdnSql).registerTempTable(mdnTable)
    sqlContext.cacheTable(mdnTable)

    val aggDF =
      sqlContext.sql( s"""
                         |select provincecode as custprovince ,companycode as vpdncompanycode, servtype, vpdndomain as domain,
                         |       type,count(distinct mdn) as usernum
                         |from ${mdnTable}  m
                         |group by provincecode,companycode, type, servtype, vpdndomain
       """.stripMargin)

    // servtype  vpdndomain
    val resultDF = aggDF.selectExpr("case when length(vpdncompanycode) = 0 or vpdncompanycode is null then 'N999999999' else vpdncompanycode end as vpdncompanycode",
      "custprovince as custprovince" ,
      "servtype,type as nettype ,vpdndomain as domain" ,
      "nvl(usernum,0) as activednum").
      withColumn("d", lit(dayid))


    //  //val cdrPartitionID = dayid.substring(2,8)
    //  ////iot_cdr_data_pdsn_d 3g
    //  //val cdrTable = "tmp_cdr_table"
    //  //val cdrDF = sqlContext.sql(
//  //
    //  //  s"""CACHE TABLE ${cdrTable} as
    //  //     |select mdn, '3G' nettype
    //  //     |from ${pdsnTable} t
    //  //     |where d='${cdrPartitionID}'
    //  //     |union all
    //  //     |select mdn, '4G' nettype
    //  //     |from ${pgwTable} t
    //  //     |where d='${cdrPartitionID}'
    //  //   """.stripMargin)

    //val raidusTable = s"${appName}_tmp_raidus"
    //sqlContext.sql(
    //  s"""CACHE TABLE ${raidusTable} as
    //     |select mdn, nettype
    //     |from ${radiusTable} t
    //     |where d='${radiusPartitionID}'
    //   """.stripMargin)

    //   //val tmpUserTable = s"${appName}_tmp_user"
    //   //sqlContext.sql(
    //   //  s"""CACHE TABLE ${tmpUserTable} as
    //   //     |select mdn,
    //   //     |case when length(companycode)=0 then 'N999999999' else companycode end  as vpdncompanycode
    //   //     |from ${userTable} t
    //   //     |where d='${userTablePartitionID}'
    //   //   """.stripMargin)
//   //
    //   //val activeUserSQL =
    //   //  s"""
    //   //     |select '${cdrPartitionID}' as d, vpdncompanycode, '3G' nettype, count(*) as activednum
    //   //     |from ${tmpUserTable} t left semi join ${cdrTable} r
    //   //     |on(t.mdn=r.mdn and r.nettype='3G')
    //   //     |group by vpdncompanycode
    //   //     |union all
    //   //     |select '${cdrPartitionID}' as d, vpdncompanycode, '4G' nettype, count(*) as activednum
    //   //     |from ${tmpUserTable} t left semi join ${cdrTable} r
    //   //     |on(t.mdn=r.mdn and r.nettype='4G')
    //   //     |group by vpdncompanycode
    //   // """.stripMargin
//   //
    // //val resultDF = sqlContext.sql(activeUserSQL)



    val fileSystem = FileSystem.newInstance(sc.hadoopConfiguration)
    val coalesceNum = 1
    val partitions="d"

    CommonETLUtils.saveDFtoPartition(sqlContext, fileSystem, resultDF, coalesceNum, partitions, dayid, outputPath, activeUserTable, appName)

  }
}
