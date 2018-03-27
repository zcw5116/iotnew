package com.zyuc.stat.iot.multiana

import com.zyuc.stat.properties.ConfigProperties
import com.zyuc.stat.utils.{DateUtils, FileUtils}
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{Logging, SparkConf, SparkContext}
import org.joda.time.DateTime

/**
  * Created by dell on 2018/1/23.
  * 10674
  * NB业务单卡汇总
  * 汇总时间端按照传入时间进行
  */
object MMENbMdnAnalysisHour extends Logging{
  def main(args: Array[String]): Unit = {
    //curtime
    val curHour = DateTime.now().toString("yyyyMMddHH")
    val sparkConf = new SparkConf()
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    sqlContext.sql("use " + ConfigProperties.IOT_HIVE_DATABASE)
    val timeType = sc.getConf.get("spark.app.timeType","h") //h
    val interval = sc.getConf.get("spark.app.interval","1") //h 7*24 = 168
    val timeid = sc.getConf.get("spark.app.timeid",curHour) //YYYYMMDDHH
    val sourceTab = sc.getConf.get("spark.app.sourceTab","iot_mme_nb_etl_h") //2017113021
    val baseStationTab = sc.getConf.get("spark.app.baseStationTab","iot_basestation_4g")
    val inputPath = sc.getConf.get("spark.app.inputPath","hdfs://EPC-IOT-ES-06:8020/hadoop/IOT/data/etl/mmenb/"+"h"+"/"+timeid.substring(0,8)+"/")
    val outputPath = sc.getConf.get("spark.app.outputPath","hdfs://EPC-IOT-ES-06:8020/hadoop/IOT/data/multiAnaly/mmenb/h/")
    val beginTime = timeid.substring(0,10)

    val endtime = DateUtils.timeCalcWithFormatConvertSafe(beginTime, "yyyyMMddHH", (interval.toInt-1)*60*60, "yyyyMMddHH")

    logInfo("##########--begintime: " + beginTime)
    logInfo("##########--endtime: " + endtime)
    logInfo("##########--interval: " + interval)
    logInfo("##########--sourceTab: " + sourceTab)
    logInfo("##########--baseStationTab: " + baseStationTab)
    logInfo("##########--inputPath: " + inputPath)
    logInfo("##########--outputPath: " + outputPath)


    val tmpMMETab = "tmpMMETab"
    val fiterDF = LoadfileByPartitinH(sqlContext,beginTime,endtime,inputPath).registerTempTable(tmpMMETab)
    val baseStationDF = sqlContext.sql(
      s""" select provname,cityname,zhlabel,enbid,RegProSign,
         |       count(*) as reqcnt,sum(succflag) as reqsucccnt,round(avg(delay),2) as avgdelay
         |from
         |       (select msisdn,m.enbid,
         |              case when b.provname is null or length(b.provname) =0 then '其他' else b.provname end as provname,
         |              case when b.cityname is null or length(b.cityname) =0 then '其他' else b.cityname end as cityname,
         |              case when b.zhlabel  is null or length(b.zhlabel) =0 then '其他' else b.zhlabel end as zhlabel,
         |              case when m.RegProSign  is null or length(m.RegProSign) =0 then '其他' else m.RegProSign end as RegProSign,
         |              case when m.RegProRst = 'succ' then 1 else 0 end as succflag,
         |              nvl(m.delay,0) as delay
         |       from   ${tmpMMETab} m
         |       left   join ${baseStationTab} b
         |       on     m.enbid = b.enbid
         |       ) m
         |group by provname,cityname,zhlabel,enbid,RegProSign,RegProRst
         |GROUPING ((provname,RegProSign),(provname,cityname,RegProSign),(provname,cityname,enbid,RegProSign))
           """.stripMargin

    )

    val resultDF = baseStationDF.selectExpr("provname",
                                            "case when cityname is null or length(cityname) =0 then -1 else cityname end as cityname",
                                            "case when enbid is null or length(enbid) =0 then -1 else enbid end as enbid",
                                            "case when zhlabel is null or length(zhlabel) =0 then -1 else zhlabel end as zhlabel",
                                            "case when RegProSign is null or length(RegProSign) =0 then -1 else RegProSign end as RegProSign",
                                            "nvl(reqcnt,0) as reqcnt","nvl(reqsucccnt,0) as reqsucccnt","nvl(avgdelay,0) as avgdelay",
                                            s"'${beginTime}' as datetime")
    val pathdayid = beginTime.substring(0,8)
    val pathtimeid = timeid
    val coalesceNum = 1
    val outputLocatoin = outputPath  + "tmp/" + pathdayid + "/"


    val fileSystem = FileSystem.newInstance(sc.hadoopConfiguration)

    resultDF.repartition(coalesceNum.toInt).write.mode(SaveMode.Overwrite).format("json").save(outputLocatoin)


    FileUtils.moveTempFilesToESpath(fileSystem,outputPath,pathtimeid,pathdayid)


  }

  def LoadfileByPartitinH  (sqlContext:SQLContext, begintime:String, endtime:String,inputPath:String): DataFrame ={
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
          logInfo("##########--ERROR:filterDF is not null bu" +
            "t sourceDF is null !!")
        }
        else if(filterDF!=null && sourceDF != null){
          filterDF = filterDF.unionAll(sourceDF)
        }

      }



    }
    filterDF

  }

}
