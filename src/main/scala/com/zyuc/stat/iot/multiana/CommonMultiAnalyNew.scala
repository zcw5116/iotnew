package com.zyuc.stat.iot.multiana

import java.text.SimpleDateFormat

import com.zyuc.stat.properties.ConfigProperties
import com.zyuc.stat.utils.{DateUtils, FileUtils}
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.{Logging, SparkConf, SparkContext}

/**
  * Created by dell on 2017/8/28.
  */
object CommonMultiAnalyNew extends Logging {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    //.setMaster("local[4]").setAppName("fd")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    sqlContext.sql("use " + ConfigProperties.IOT_HIVE_DATABASE)
    val sumitemtype = sc.getConf.get("spark.app.item.type") //"authlog_3gaaa;authlog_4gaaa,authlog_pdsn"
    val begintime = sc.getConf.get("spark.app.begintime") //20170826010000 yyyymmddhhmiss
    //val endtime = sc.getConf.get("spark.app.endtime")//20170826020000 yyyymmddhhmiss
    val interval = sc.getConf.get("spark.app.interval")//15
    val cyctype = sc.getConf.get("spark.app.cyctype")// min/h/d/w/m
    val userTable = sc.getConf.get("spark.app.user.table")     // "iot_customer_userinfo"
    val userTablePartitionDayid = sc.getConf.get("spark.app.user.userTablePatitionDayid")  //  "20170801"
    val inputPath = sc.getConf.get("spark.app.inputPath") // hdfs://EPC-IOT-ES-06:8020/hadoop/IOT/data/multiAna/authlog/15min/;
    val outputPath = sc.getConf.get("spark.app.outputPath") // hdfs://EPC-IOT-ES-06:8020/hadoop/IOT/data/multiAna/authlog/15min/;
    val localOutputPath =  sc.getConf.get("spark.app.jsonpath") // /slview/test/limm/multiAna/authlog/15min/json/;


    //判断统计起始和截止时间
    val intervals = interval.toInt * 60  //周期
    var beginStr:String = null
    var endStr:String = null
    var intervalbegin:Int = 0
    var intervalend:Int = 0
    var timeformat:String = null           // json数据中的时间格式
    var datatime:String = null            //json数据中显示的时间
    var timeid:String = null              //临时目录中的时间子目录
    var dayid:String = begintime.substring(0,8)  //数据最终存放目录中的时间子目录

    var endtime:String = null

    logInfo("##########--sumitemtype: " + sumitemtype)
    logInfo("##########--begintime: " + begintime)

    logInfo("##########--interval: " + interval)
    logInfo("##########--cyctype: " + cyctype)
    logInfo("##########--inputPath: " + inputPath)
    //每次执行一个周期，最小周期为小时

    if(cyctype == "min"){
      intervalbegin = intervals*(-1)
      intervalend = intervals*(-1)+60*60
      timeformat = "yyyyMMddHHmm"
      timeid = begintime.substring(0,10)
      dayid = timeid.substring(0,8)
      endtime = DateUtils.timeCalcWithFormatConvertSafe(begintime.substring(0,12), "yyyyMMddHHmm", 1*60*60, "yyyyMMddHHmmss")
    }else if(cyctype == "h"){
      intervalbegin = intervals*(-1)
      intervalend = intervals*(-1)
      timeformat = "yyyyMMddHH"
      timeid = begintime.substring(0,10)
      dayid = timeid.substring(0,8)
      endtime = DateUtils.timeCalcWithFormatConvertSafe(begintime.substring(0,12), "yyyyMMddHHmm", 1*60*60, "yyyyMMddHHmmss")
    }else if(cyctype == "d"){
      intervalbegin = 0
      intervalend = 0
      timeformat = "yyyyMMdd"
      timeid = begintime.substring(0,8)
      datatime = begintime.substring(0,8)
      endtime = begintime
    }else if(cyctype == "w"){
      intervalbegin = 0
      intervalend = 6*24*60*60
      timeformat = "yyyyMMdd"
      timeid = begintime.substring(0,8)
      datatime = begintime.substring(0,8)
      endtime = DateUtils.timeCalcWithFormatConvertSafe(begintime.substring(0,12), "yyyyMMddHHmm", 7*60*60, "yyyyMMddHHmmss")
    }else if(cyctype == "m"){
      intervalbegin = 0
      val monthid = begintime.substring(4,6)
      if(monthid == "02"){
        intervalend = 27*24*60*60
      }else if(monthid == "01"|| monthid == "03"|| monthid == "05"|| monthid == "07"|| monthid == "08"|| monthid == "10"|| monthid == "12"){
        intervalend = 30*24*60*60
      }else{
        intervalend = 29*24*60*60
      }
      timeid = begintime.substring(0,6)
      timeformat = "yyyyMM"
      datatime = begintime.substring(0,6)
      dayid = dayid.substring(0,6)
      endtime = DateUtils.timeCalcWithFormatConvertSafe(begintime.substring(0,12), "yyyyMMddHHmm", intervalend, "yyyyMMddHHmmss")
    }
    logInfo("##########--endtime: " + endtime)
    beginStr = DateUtils.timeCalcWithFormatConvertSafe(begintime.substring(0,12), "yyyyMMddHHmm", intervalbegin, "yyyyMMddHHmmss")
    endStr = DateUtils.timeCalcWithFormatConvertSafe(begintime.substring(0,12), "yyyyMMddHHmm", intervalend, "yyyyMMddHHmmss")

    val beginStrH = beginStr
    val endStrH = endStr
    val beginStrD = beginStr.substring(0,8)
    val endStrD = endStr.substring(0,8)

    //获取开始时间戳
    val fm = new SimpleDateFormat("yyyyMMddHHmmss")
    val dt = fm.parse(begintime.substring(0,14))
    val begintimestamp = dt.getTime

    //获取汇总类型
    //val ItmeName = sumitemtype.substring(0,sumitemtype.lastIndexOf("_") + 1) //"authlog"
    //val subItmeName = sumitemtype.substring(sumitemtype.lastIndexOf("_") + 1) //"4gaaa"
    //val ItmeName = sumitemtype.split("_",2)(0)
    logInfo("##########--sumitemtype: " + sumitemtype)
    logInfo("##########--begintime: " + begintime)
    logInfo("##########--endtimeH: " + endStrH)
    logInfo("##########--endtimeD: " + endStrD)
    logInfo("##########--begintimestamp: " + begintimestamp)
    logInfo("##########--intervals: " + intervals)

    //企业表
    val companyTabName = "tmpCompanyInfo"
    val companyCol = sqlContext.sql("select companycode ,provincecode from iot_basic_company_and_domain ").collect()
    val companyBd =  sc.broadcast(companyCol)
    val companyValue = companyBd.value
    import sqlContext.implicits._
    sc.parallelize(companyValue).map(x=>(x(0).toString, x(1).toString)).toDF("companycode", "provincecode").registerTempTable(companyTabName)

    //企业表与用户表关联，去除用户表中省份为空的企业
    val filtertable = "filtertable"
    val rstUserTab = "rstUserTab"
    sqlContext.sql(
      s"""   CACHE TABLE ${rstUserTab} as
         |        select u.mdn,u.imei,u.isvpdn,u.isdirect ,u.vpdndomain,u.iscommon,u.imsicdma,
         |               case when length(c.provincecode)=0 or c.provincecode is null then 'N999999999' else c.provincecode end  as provincecode,
         |               case when length(u.companycode)=0 then 'P999999999' else u.companycode end  as companycode
         |        from   ${userTable}  u
         |        left join ${companyTabName} c
         |        on c.companycode = u.companycode
         |        where  d = '${userTablePartitionDayid}'
       """.stripMargin)


    //从小时或天表取数据
    // 按照粒度汇总数据,一次汇一个周期数据，开始时间作为统计时间，分钟为每小时统计一次,12~13点汇总12点数据
    var filterDF:DataFrame =null
    var resultDF:DataFrame = null

    var outputfsSuffix:String = null
    var outputsubLSuffix:String = null
    var outputLSuffix:String = null

    if(cyctype == "min" || cyctype == "h"){



      filterDF = LoadfileByPartitinH(sumitemtype,sqlContext,beginStrH,endStrH,inputPath,intervals)

      filterDF.registerTempTable(filtertable)
      resultDF = SummarySourceHour(sqlContext,filtertable,sumitemtype,begintimestamp,intervals,rstUserTab,userTablePartitionDayid,timeformat)


    }else if(cyctype == "d" || cyctype == "w"|| cyctype == "m"){

      filterDF = LoadfileByPartitinD(sqlContext,beginStrD,endStrD,inputPath,intervals)
      filterDF.registerTempTable(filtertable)
      if(filterDF == null){
        logInfo("##########--filterDF: is null")
      }
      resultDF = SummarySourceDay(sqlContext,filtertable,sumitemtype,datatime,intervals,rstUserTab,userTablePartitionDayid)
      if(resultDF == null){
        logInfo("##########--resultDF: is null")
      }

    }
    // 修改列名
    //resultDF = resultDF.withColumn("custprovince",resultDF.col("provincecode")).drop(resultDF.col("provincecode"))

    // 文件写入JSON
    val coalesceNum = 1
    val outputLocatoin = outputPath + "tmp/" +timeid+"/"
    //val localpath =  localOutputPath

    val fileSystem = FileSystem.newInstance(sc.hadoopConfiguration)

    resultDF.repartition(coalesceNum.toInt).write.mode(SaveMode.Overwrite).format("json").save(outputLocatoin)

    FileUtils.moveTempFilesToESpath(fileSystem,outputPath,timeid,dayid)
    //FileUtils.downFilesToLocal(fileSystem, outputLocatoin, localpath + "/", outputLSuffix, ".json")

    sc.stop()


  }

  // 按分区加载数据
  def LoadfileByPartitinH  (partitioncolname:String,sqlContext:SQLContext, begintime:String, endtime:String,inputPath:String,intervals:Int): DataFrame ={
    var filterDF:DataFrame = null
    var sourceDF:DataFrame = null
    //获取汇总在分区的时间字段

    if( begintime == "" ||  endtime == "" || inputPath == ""){
      return filterDF
    }
    val intervalHourNums = (DateUtils.timeInterval(begintime.substring(0,10), endtime.substring(0,10), "yyyyMMddHH")) / 3600
    var inputLocation = new Array[String](intervalHourNums.toInt+1)
    logInfo("##########--intervalHourNum: " + intervalHourNums)
    if (begintime.substring(2,10) == endtime.substring(2,10)) {
      inputLocation(0) = inputPath + "/d=" + begintime.substring(2,8) + "/h=" + begintime.substring(8,10)
      //inputLocation(0) = inputPath + "/d=" + begintime.substring(2,8) + "/h=" + begintime.substring(9,10)
      logInfo("##########--inputpath: " + inputLocation(0))
      filterDF = sqlContext.read.format("orc").load(inputLocation(0))
    }else {

      for (i <- 0 to intervalHourNums.toInt) {
        val curtime = DateUtils.timeCalcWithFormatConvertSafe(begintime.substring(0,10), "yyyyMMddHH", i * 60 * 60, "yyyyMMddHH")
        val curH = curtime.substring(8)
        val curD = curtime.substring(0,8)
        inputLocation(i) = inputPath + "/d=" + curD.substring(2) + "/h=" + curH
        //inputLocation(i) = inputPath + "/d=" + curD.substring(2) + "/h=" + curH.substring(1)
        logInfo("##########--inputpath:" + i +"--" + inputLocation(i))
        //logInfo("##########--begintime:" + i +"--" + begintime
        //if(i==0){
        //   sourceDF = sqlContext.read.format("orc").load(inputLocation(i))
        //}else if(i == intervalHourNums){
        //   sourceDF = sqlContext.read.format("orc").load(inputLocation(i))
        //}else{
        //   sourceDF = sqlContext.read.format("orc").load(inputLocation(i))
        //}
        sourceDF = sqlContext.read.format("orc").load(inputLocation(i))
        if (filterDF==null && sourceDF==null)
        {
          logInfo("##########--ERROR:filterDF is null and sourceDF is null !!")

        }else if(filterDF==null && sourceDF != null){
          filterDF=sourceDF
        }
        else if(filterDF!=null && sourceDF == null){
          logInfo("##########--ERROR:filterDF is not null but sourceDF is null !!")
        }
        else if(filterDF!=null && sourceDF != null){
          filterDF = filterDF.unionAll(sourceDF)
        }

      }
      if(partitioncolname == "auth_3g" || partitioncolname == "auth_4g"  || partitioncolname == "auth_vpdn"  ){

        filterDF = filterDF.filter("authtime>=" + begintime).
          filter( "authtime<" + endtime)

      }else if(partitioncolname == "mme"){
        filterDF = filterDF.filter(s"starttime >= '$begintime'").
          filter( s"starttime < '$endtime'")

      }

    }
    filterDF.show(2)
    filterDF

  }

  def LoadfileByPartitinD  (sqlContext:SQLContext, begintime:String, endtime:String,inputPath:String,intervals:Int): DataFrame ={
    var filterDF:DataFrame = null
    if( begintime == "" ||  endtime == "" || inputPath == ""){
      logInfo("##########--filterDF is null" )
      return filterDF

    }
    val intervalDayNums = (DateUtils.timeInterval(begintime.substring(0,8), endtime.substring(0,8), "yyyyMMdd")) / (3600*24)
    var inputLocation = new Array[String](intervalDayNums.toInt+1)
    if (begintime.substring(0,8) == endtime.substring(0,8)) {
      inputLocation(0) = inputPath + "/d=" + begintime.substring(2,8)
      logInfo("##########--inputpath:" +  inputLocation(0))
      filterDF = sqlContext.read.format("orc").load(inputLocation(0))
      if(filterDF == null){
        logInfo("##########--filterDF: is null")
      }
    }else {

      for (i <- 0 to intervalDayNums.toInt) {
        val curD = DateUtils.timeCalcWithFormatConvertSafe(begintime.substring(0,8), "yyyyMMdd", i * 60 * 60*24, "yyMMdd")
        inputLocation(i) = inputPath + "/d=" + curD.substring(2)
        logInfo("##########--inputpath:" + i +"--" + inputLocation(i))
        val sourceDF = sqlContext.read.format("orc").load(inputLocation(i))

        if (filterDF==null && sourceDF==null)
        {
          logInfo("##########--ERROR:filterDF is null and sourceDF is null !!")

        }else if(filterDF==null && sourceDF != null){
          filterDF=sourceDF
        }
        else if(filterDF!=null && sourceDF == null){
          logInfo("##########--ERROR:filterDF is not null but sourceDF is null !!")
        }
        else if(filterDF !=null && sourceDF != null){
          filterDF = filterDF.unionAll(sourceDF)
        }
      }

    }
    filterDF

  }
  def SummarySourceHour (sqlContext:SQLContext,filtertable:String,ItmeName:String,btimestamp:Long,intervals:Int,userTable:String,userTablePartitionDayid:String,timeformat:String):DataFrame={

    var resultDF:DataFrame = null
    var companyDF:DataFrame = null
    if(ItmeName == "auth_3g"){

      resultDF=
        sqlContext.sql(s"""
           |select provincecode as custprovince,companycode,datetime,servtype,domain,result,auth_result as errorcode,
           |       sum(rqsnum) as requirecnt,
           |       sum(result_f) as errcnt,
           |       count(distinct mdn) as mdncnt,
           |       count(distinct  errmdn) as errmdncnt
           |from (
           |    select u.provincecode,
           |           u.companycode,
           |           '3g' type,
           |           u.mdn,
           |           a.auth_result,
           |           a.nasportid  as bsid,
           |           (case when u.isdirect=1 then 'D' when u.isvpdn=1 and array_contains(split(vpdndomain,','), regexp_extract(a.nai_sercode, '^.+@(.*)', 1))
           |           then 'C' else 'P' end) as servtype,
           |           (case when u.isdirect != 1  and u.isvpdn=1 and array_contains(split(vpdndomain,','), regexp_extract(a.nai_sercode, '^.+@(.*)', 1))
           |           then regexp_extract(a.nai_sercode, '^.+@(.*)', 1) else '-1' end) as domain,
           |           from_unixtime(ceil((unix_timestamp(a.authtime,"yyyyMMddHHmmss") - ${btimestamp})/${intervals})*${intervals}+${btimestamp},"${timeformat}")
           |           as datetime,
           |           a.result,
           |           case when a.result = 'failed'then u.mdn end as errmdn,
           |           case when a.result = 'failed' then 1 else 0 end result_f,
           |           1 as rqsnum
           |    from   ${userTable} u, ${filtertable} a
           |    where   u.imsicdma = a.imsicdma
           |) t
           |group by provincecode,companycode,domain,servtype,result,auth_result,datetime
           |
           |
         """.stripMargin)
      resultDF.show(5)

      //  )
    }else if(ItmeName == "auth_4g" || ItmeName== "auth_vpdn"){
      resultDF=
        sqlContext.sql(s"""
                          |select provincecode as custprovince,companycode,datetime,servtype,mdndomain as domain,result,auth_result as errorcode,
                          |       sum(rqsnum) as requirecnt,
                          |       sum(result_f) as errcnt,
                          |       count(distinct mdn) as mdncnt,
                          |       count(distinct errmdn ) as errmdncnt
                          |from (
                          |    select u.provincecode,
                          |           u.companycode,
                          |           '4g' type,
                          |           u.mdn,
                          |           a.auth_result,
                          |           case when ${ItmeName} = "auth_4g" then a.nasportid else '-1' end as bsid,
                          |           (case when u.isdirect=1 then 'D' when u.isvpdn=1 and array_contains(split(vpdndomain,','), regexp_extract(a.nai_sercode, '^.+@(.*)', 1))
                          |           then 'C' else 'P' end) as servtype,
                          |           (case when u.isdirect != 1  and u.isvpdn=1 and array_contains(split(vpdndomain,','), regexp_extract(a.nai_sercode, '^.+@(.*)', 1))
                          |           then regexp_extract(a.nai_sercode, '^.+@(.*)', 1) else '-1' end) as mdndomain,
                          |           from_unixtime(ceil((unix_timestamp(a.authtime,"yyyyMMddHHmmss") - ${btimestamp})/${intervals})*${intervals}+${btimestamp},"${timeformat}") as datetime,
                          |           result,
                          |           case when a.result = 'failed'then u.mdn end as errmdn,
                          |           case when a.result = 'failed' then 1 else 0 end result_f,
                          |           1 as rqsnum
                          |    from   ${userTable} u, ${filtertable} a
                          |    where   u.mdn = a.mdn
                          |) t
                          |group by provincecode,companycode,mdndomain,servtype,result,auth_result,datetime
         """.stripMargin)
      resultDF.show(5)

    }else if(ItmeName == "mme"){

      val mdnSql =
        s"""
           |    select u.provincecode,
           |           u.companycode ,
           |           u.mdn,  u.vpdndomain, m.pcause, m.result, u.isdirect, u.isvpdn, u.iscommon,
           |           m.devicetype,
           |           m.modelname,
           |           m.pcause as errorcode,
           |           (case when m.mmetype in('hwmm','hwsm') then conv(substr(m.enbid,3), 16, 10) else m.enbid end) as enbid,
           |           case when result="failed" then 1 else 0 end fail_cnt,
           |           case when result="failed" then u.mdn end fail_mdn,
           |           result,
           |           from_unixtime(ceil((unix_timestamp(m.starttime,"yyyyMMddHHmmss") - ${btimestamp})/${intervals})*${intervals}+${btimestamp},"${timeformat}") as datetime
           |    from   ${filtertable} m inner join ${userTable} u
           |           on( m.msisdn=u.mdn)
           |    where  m.isattach=1
       """.stripMargin
      val mdnTable = "mdnTable_tmp"
      sqlContext.sql(mdnSql).registerTempTable(mdnTable)
      sqlContext.cacheTable(mdnTable)
       resultDF =
        sqlContext.sql(s"""
           |select provincecode as custprovince,companycode, servtype, vpdndomain as domain,datetime,
           |       devicetype,modelname,errorcode,
           |       sum(reqcnt) as requirecnt,
           |       sum(case when result="success" then 0 else reqcnt end)  as errcnt,
           |       count(distinct mdn) as mdncnt,
           |       count(distinct(case when result="failed" then mdn end)) as errmdncnt
           |from
           |(
           |    select t.errorcode,t.devicetype,t.modelname,t.provincecode,t.companycode, 'D' as servtype, "-1" as vpdndomain , t.mdn, t.result,t.datetime,
           |           count(*) as reqcnt
           |    from   ${mdnTable} t
           |    where  t.isdirect='1'
           |    group by t.provincecode, t.errorcode,t.devicetype,t.modelname,t.companycode, t.vpdndomain, t.mdn, t.result,t.datetime
           |    union all
           |    select errorcode,devicetype,modelname,provincecode,companycode, servtype, c.vpdndomain, mdn, result, datetime,reqcnt
           |    from
           |    (
           |        select  t.errorcode,t.devicetype,t.modelname,t.provincecode,t.companycode, 'C' as servtype, t.vpdndomain, t.mdn, t.result,t.datetime,
           |                count(*) as reqcnt
           |        from    ${mdnTable} t
           |        where   t.isvpdn='1'
           |        group by t.errorcode,t.devicetype,t.modelname,t.provincecode, t.companycode, t.vpdndomain, t.mdn, t.result,t.datetime
           |    ) s lateral view explode(split(s.vpdndomain,',')) c as vpdndomain
           |    union all
           |    select t.errorcode,t.devicetype,t.modelname,t.provincecode,t.companycode, 'C' as servtype, '-1' as vpdndomain, t.mdn, t.result,t.datetime,
           |           count(*) as reqcnt
           |    from   ${mdnTable} t
           |    where  t.isvpdn='1'
           |    group by t.errorcode,t.devicetype,t.modelname,t.provincecode,t.companycode, t.vpdndomain, t.mdn, t.result,t.datetime
           |    union all
           |    select t.errorcode,t.devicetype,t.modelname,t.provincecode,t.companycode, 'P' as servtype, "-1" as vpdndomain, t.mdn, t.result,t.datetime,
           |           count(*) as reqcnt
           |    from   ${mdnTable} t
           |    where  t.iscommon='1'
           |    group by t.provincecode,t.errorcode,t.devicetype,t.modelname,t.companycode, t.vpdndomain, t.mdn, t.result,t.datetime
           |    union all
           |    select t.errorcode,t.devicetype,t.modelname,t.provincecode,t.companycode, '-1' as servtype, "-1" as vpdndomain, t.mdn, t.result,t.datetime,
           |           count(*) as reqcnt
           |    from   ${mdnTable} t
           |    group by t.provincecode,t.errorcode,t.devicetype,t.modelname,t.companycode, t.vpdndomain, t.mdn, t.result,t.datetime
           |) m
           |group by provincecode,errorcode,companycode, servtype, vpdndomain,datetime,devicetype,modelname
       """.stripMargin)
      //resultDF = filterDF.groupBy(
      //  when (length(filterDF.col("custprovince"))===0,"其他").when(filterDF.col("custprovince").isNull,"其他").otherwise(filterDF.col("custprovince")).alias("custprovince"),
      //  when (length(filterDF.col("vpdncompanycode"))===0,"P999999999").when(filterDF.col("vpdncompanycode").isNull,"N999999999").otherwise(filterDF.col("vpdncompanycode")).alias("companycode"),
      //  filterDF.col("province"),
      //  when(filterDF.col("devicetype").isNull,"").otherwise(filterDF.col("devicetype")).alias("devicetype"),
      //  when(filterDF.col("modelname").isNull,"").otherwise(filterDF.col("modelname")).alias("modelname"),
      //  from_unixtime(ceil((unix_timestamp(filterDF.col("starttime"),"yyyy-MM-dd HH:mm:ss.SSS") - btimestamp)/intervals)*intervals+btimestamp,s"$timeformat").as("datetime"),
      //  filterDF.col("result"),
      //  filterDF.col("pcause").as("errorcode")).
      //  agg(
      //    sum(when (length(filterDF.col("mdn"))>0,1).otherwise(0)).alias("requirecnt"),
      //    sum(when (filterDF.col("result")==="failed" and length(filterDF.col("mdn"))>0,1).otherwise(0)).alias("errcnt"),
      //    countDistinct((when(filterDF.col("result")==="failed",filterDF.col("mdn")))).as("errmdncnt"),
      //    countDistinct(filterDF.col("mdn")).as("mdncnt")
      //  )
    }else if(ItmeName == "flow"){


    }




    resultDF
  }


  def SummarySourceDay (sqlContext:SQLContext,filtertable:String,ItmeName:String,dayid:String,intervals:Int,userTable:String,userTablePartitionDayid:String):DataFrame={

    var resultDF:DataFrame = null

    if(ItmeName == "auth_3g"){
      resultDF =
        sqlContext.sql(
        s"""
           select provincecode as custprovince,companycode,datetime,servtype,domain,result,auth_result as errorcode,
           |       sum(rqsnum) as requirecnt,
           |       sum(result_f) as errcnt,
           |       count(distinct mdn) as mdncnt,
           |       count(distinct  errmdn) as errmdncnt
           |from (
           |select u.provincecode,
           |       u.companycode,
           |       '3g' type,
           |       u.mdn,
           |       a.auth_result,
           |       a.nasportid  as bsid,
           |       (case when u.isdirect=1 then 'D' when u.isvpdn=1 and array_contains(split(vpdndomain,','), regexp_extract(a.nai_sercode, '^.+@(.*)', 1))
           |       then 'C' else 'P' end) as servtype,
           |       (case when u.isdirect != 1  and u.isvpdn=1 and array_contains(split(vpdndomain,','), regexp_extract(a.nai_sercode, '^.+@(.*)', 1))
           |       then regexp_extract(a.nai_sercode, '^.+@(.*)', 1) else '-1' end) as domain,
           |       ${dayid} as datetime,
           |       a.result,
           |       case when a.result = 'failed'then u.mdn end as errmdn,
           |       case when a.result = 'failed' then 1 else 0 end result_f,
           |       1 as rqsnum
           |from   ${userTable} u, ${filtertable} a
           |where   u.imsicdma = a.imsicdma
           |) t
           |group by provincecode,companycode,domain,servtype,result,auth_result,datetime
           |
           |
         """.stripMargin)

    }else if(ItmeName == "auth_4g" || ItmeName== "auth_vpdn"){
      resultDF=
        sqlContext.sql(s"""
                          select provincecode as custprovince,companycode,datetime,servtype,mdndomain as domain,result,auth_result as errorcode,
                          |       sum(rqsnum) as requirecnt,
                          |       sum(result_f) as errcnt,
                          |       count(distinct mdn) as mdncnt,
                          |       count(distinct errmdn ) as errmdncnt
                          |from (
                          |select u.provincecode,
                          |       u.companycode,
                          |       '4g' type,
                          |       u.mdn,
                          |       a.auth_result,
                          |       case when ${ItmeName} = "authlog_4gaaa" then a.nasportid else '-1' end as bsid,
                          |       (case when u.isdirect=1 then 'D' when u.isvpdn=1 and array_contains(split(vpdndomain,','), regexp_extract(a.nai_sercode, '^.+@(.*)', 1))
                          |       then 'C' else 'P' end) as servtype,
                          |       (case when u.isdirect != 1  and u.isvpdn=1 and array_contains(split(vpdndomain,','), regexp_extract(a.nai_sercode, '^.+@(.*)', 1))
                          |       thenregexp_extract(a.nai_sercode, '^.+@(.*)', 1) else '-1' end) as mdndomain,
                          |       result,
                          |       ${dayid} as datetime,
                          |       case when a.result = 'failed'then u.mdn end as errmdn,
                          |       case when a.result = 'failed' then 1 else 0 end result_f,
                          |       1 as rqsnum
                          |from   ${userTable} u, ${filtertable} a
                          |where   u.mdn = a.mdn
                          |) t
                          |group by provincecode,companycode,mdndomain,servtype,result,auth_result,datetime
         """.stripMargin)
      resultDF.show(5)
    }else if(ItmeName == "mme"){


      val mdnSql =
        s"""
           |    select u.provincecode,
           |           u.companycode ,
           |           u.mdn,  u.vpdndomain, m.pcause, m.result, u.isdirect, u.isvpdn, u.iscommon,
           |           m.devicetype,
           |           m.modelname,
           |           m.pcause as errorcode,
           |           (case when m.mmetype in('hwmm','hwsm') then conv(substr(m.enbid,3), 16, 10) else m.enbid end) as enbid,
           |           case when result="failed" then 1 else 0 end fail_cnt,
           |           result,
           |           ${dayid} as datetime
           |    from   ${filtertable} m inner join ${userTable} u
           |           on( m.msisdn=u.mdn)
           |    where  m.isattach=1
       """.stripMargin
      val mdnTable = "mdnTable_tmp"
      sqlContext.sql(mdnSql).registerTempTable(mdnTable)
      //sqlContext.cacheTable(mdnTable)
      logInfo("##########--begin agg... " )
      resultDF =
        sqlContext.sql(s"""
                         select provincecode as custprovince,companycode, servtype, vpdndomain as domain,datetime,
                          |       devicetype,modelname,errorcode,
                          |       sum(reqcnt) as requirecnt,
                          |       sum(case when result="success" then 0 else reqcnt end)  as errcnt,
                          |       count(distinct mdn) as mdncnt,
                          |       count(distinct(case when result="failed" then mdn end)) as errmdncnt
                          |from
                          |(
                          |    select t.errorcode,t.devicetype,t.modelname,t.provincecode,t.companycode, 'D' as servtype, "-1" as vpdndomain , t.mdn, t.result,t.datetime,
                          |           count(*) as reqcnt
                          |    from   ${mdnTable} t
                          |    where  t.isdirect='1'
                          |    group by t.provincecode,t.errorcode,t.devicetype,t.modelname,t.companycode, t.vpdndomain, t.mdn, t.result,t.datetime
                          |    union all
                          |    select errorcode,devicetype,modelname,provincecode,companycode, servtype, c.vpdndomain, mdn, result, reqcnt
                          |    from
                          |    (
                          |        select  t.errorcode,t.devicetype,t.modelname,t.provincecode,t.companycode, 'C' as servtype, t.vpdndomain, t.mdn, t.result,t.datetime,t.datetime,
                          |                count(*) as reqcnt
                          |        from    ${mdnTable} t
                          |        where   t.isvpdn='1'
                          |        group by t.errorcode,t.devicetype,t.modelname,t.provincecode, t.companycode, t.vpdndomain, t.mdn, t.result,t.datetime
                          |    ) s lateral view explode(split(s.vpdndomain,',')) c as vpdndomain
                          |    union all
                          |    select t.errorcode,t.devicetype,t.modelname,t.provincecode,t.companycode, 'C' as servtype, '-1' as vpdndomain, t.mdn, t.result,t.datetime,
                          |           count(*) as reqcnt
                          |    from   ${mdnTable} t
                          |    where  t.isvpdn='1'
                          |    group by t.errorcode,t.devicetype,t.modelname,t.provincecode,t.companycode, t.vpdndomain, t.mdn, t.result,t.datetime
                          |    union all
                          |    select t.errorcode,t.devicetype,t.modelname,t.provincecode,t.companycode, 'P' as servtype, "-1" as vpdndomain, t.mdn, t.result,t.datetime,
                          |           count(*) as reqcnt,t.datetime
                          |    from   ${mdnTable} t
                          |    where  t.iscommon='1'
                          |    group by t.errorcode,t.devicetype,t.modelname,t.provincecode,t.companycode, t.vpdndomain, t.mdn, t.result,t.datetime
                          |    union all
                          |    select t.errorcode,t.devicetype,t.modelname,t.provincecode,t.companycode, '-1' as servtype, "-1" as vpdndomain, t.mdn, t.result,t.datetime,
                          |           count(*) as reqcnt
                          |    from   ${mdnTable} t
                          |    group by t.errorcode,t.devicetype,t.modelname,t.provincecode,t.companycode, t.vpdndomain, t.mdn, t.result,t.datetime
                          |) m
                          |group by errorcode,provincecode,companycode, servtype, vpdndomain,datetime,devicetype,modelname
       """.stripMargin)

      //resultDF = filterDF.groupBy(
      //  when (length(filterDF.col("custprovince"))===0,"其他").when(filterDF.col("custprovince").isNull,"其他").otherwise(filterDF.col("custprovince")).alias("custprovince"),
      //  when (length(filterDF.col("vpdncompanycode"))===0,"N999999999").when(filterDF.col("vpdncompanycode").isNull,"N999999999").otherwise(filterDF.col("vpdncompanycode")).alias("companycode"),
      //  filterDF.col("province"),
      //  when(filterDF.col("devicetype").isNull,"").otherwise(filterDF.col("devicetype")).alias("devicetype"),
      //  when(filterDF.col("modelname").isNull,"").otherwise(filterDF.col("modelname")).alias("modelname"),
      //  filterDF.col("result"),
      //  filterDF.col("pcause").as("errorcode")).
      //  agg(
      //    sum(when (length(filterDF.col("mdn"))>0,1).otherwise(0)).alias("requirecnt"),
      //    sum(when (filterDF.col("result")==="failed" and length(filterDF.col("mdn"))>0,1).otherwise(0)).alias("errcnt"),
      //    countDistinct((when(filterDF.col("result")==="failed",filterDF.col("mdn")))).as("errmdncnt"),
      //    countDistinct(filterDF.col("mdn")).as("mdncnt")
      //  ).withColumn("datetime",lit(dayid))
    }else if(ItmeName == "flow"){
      resultDF

    }



    resultDF
  }




}
