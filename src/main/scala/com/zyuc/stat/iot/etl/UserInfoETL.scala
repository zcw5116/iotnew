package com.zyuc.stat.iot.etl

import com.zyuc.stat.iot.etl.OperalogETL.logInfo
import com.zyuc.stat.iot.etl.util.UserInfoConverterUtils
import com.zyuc.stat.properties.ConfigProperties
import com.zyuc.stat.utils.DateUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.{Logging, SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.functions.broadcast

/**
  * Created by zhoucw on 17-7-27.
  * @deprecated
  */
case class VPDNProv(custProvince:String, vpdncompanycode:String)

object UserInfoETL extends Logging {

  val SYCNTYPE_FULL = "full"
  val SYCNTYPE_INCR = "incr"

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    //.setMaster("local[4]").setAppName("fd")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    sqlContext.sql("use " + ConfigProperties.IOT_HIVE_DATABASE)
    val appName = sc.getConf.get("spark.app.appName", "UserInfoETL")
    val dataDayid = sc.getConf.get("spark.app.dataDayid")
    // val dataDayid = "20170714"
    val userTable = sc.getConf.get("spark.app.userTable", "iot_customer_userinfo")
    // val userTable = "iot_customer_userinfo"
    val syncType = sc.getConf.get("spark.app.syncType", "incr")
    val inputPath = sc.getConf.get("spark.app.inputPath")
    //val inputPath = "/hadoop/IOT/ANALY_PLATFORM/BasicData/UserInfo/"
    val outputPath = sc.getConf.get("spark.app.outputPath")
    //val outputPath = "/hadoop/IOT/ANALY_PLATFORM/BasicData/output/UserInfo/"
    val fileWildcard = sc.getConf.get("spark.app.fileWildcard")
    // val fileWildcard = "all_userinfo_qureyes_20170714*"
    // val fileWildcard = "incr_userinfo_qureyes_20170715*"
    val vpdnInput = sc.getConf.get("spark.app.vpdnInput") //   /hadoop/IOT/ANALY_PLATFORM/BasicData/VPDNProvince/
    val vpdnWildcard = sc.getConf.get("spark.app.vpdnWildcard") //   vpdninfo.txt
    val fileSystem = FileSystem.get(sc.hadoopConfiguration)
    val fileLocation = inputPath + "/" + fileWildcard
    val crttime = DateUtils.getNowTime("yyyy-MM-dd HH:mm:ss")

    val textDF = sqlContext.read.format("text").load(fileLocation)
    val userDF = sqlContext.createDataFrame(textDF.map(x => UserInfoConverterUtils.parseLine(x.getString(0))).filter(_.length != 1), UserInfoConverterUtils.struct)

    val tmpTable = appName + dataDayid
    userDF.registerTempTable(tmpTable)



    var resultDF: DataFrame = null
    if (syncType == SYCNTYPE_FULL) {
      // 将一条记录中的多个企业拆分成多行
      val tmpSql =
        s"""
           |select distinct mdn,imsicdma,imsilte,iccid,imei,company,companycode as vpdncompanycode,nettype,vpdndomain,isvpdn,subscribetimeaaa,
           |subscribetimehlr,subscribetimehss,subscribetimepcrf,firstactivetime,userstatus,atrbprovince,userprovince, '${crttime}' as crttime
           |from ${tmpTable} t lateral view explode(split(t.vpdncompanycode,',')) c as companycode where mdn is not null
       """.stripMargin

      resultDF = sqlContext.sql(tmpSql)
    } else if (syncType == SYCNTYPE_INCR) {

      val preDayid = DateUtils.timeCalcWithFormatConvertSafe(dataDayid, "yyyyMMdd", -1 * 24 * 60 * 60, "yyyyMMdd")
      val incrTable = "incrTable" + preDayid
      val preDayUserTable = "preDayUserTable"
      sqlContext.table(userTable).filter(s"d = ${preDayid}").registerTempTable(preDayUserTable)
      sqlContext.sql(s"select * from ${userTable} where d='${preDayid}' ").registerTempTable(preDayUserTable)

      val joinSql =
        s"""
           |select  nvl(t.mdn, u.mdn) as mdn,
           |        if(t.mdn is null, u.imsicdma,t.imsicdma) as imsicdma,
           |        if(t.mdn is null, u.imsilte,t.imsilte) as imsilte,
           |        if(t.mdn is null, u.iccid,t.iccid) as iccid,
           |        if(t.mdn is null, u.imei,t.imei) as imei,
           |        if(t.mdn is null, u.company,t.company) as company,
           |        if(t.mdn is null, u.vpdncompanycode,t.vpdncompanycode) as vpdncompanycode,
           |        if(t.mdn is null, u.nettype,t.nettype) as nettype,
           |        if(t.mdn is null, u.vpdndomain,t.vpdndomain) as vpdndomain,
           |        if(t.mdn is null, u.isvpdn,t.isvpdn) as isvpdn,
           |        if(t.mdn is null, u.subscribetimeaaa,t.subscribetimeaaa) as subscribetimeaaa,
           |        if(t.mdn is null, u.subscribetimehlr,t.subscribetimehlr) as subscribetimehlr,
           |        if(t.mdn is null, u.subscribetimehss,t.subscribetimehss) as subscribetimehss,
           |        if(t.mdn is null, u.subscribetimepcrf,t.subscribetimepcrf) as subscribetimepcrf,
           |        if(t.mdn is null, u.firstactivetime,t.firstactivetime) as firstactivetime,
           |        if(t.mdn is null, u.userstatus,t.userstatus) as userstatus,
           |        if(t.mdn is null, u.atrbprovince,t.atrbprovince) as atrbprovince,
           |        if(t.mdn is null, u.userprovince,t.userprovince) as userprovince,
           |        if(t.mdn is null, u.crttime,'${crttime}') as crttime
           |        from ${preDayUserTable} u full outer join  ${tmpTable} t
           |        on(u.mdn=t.mdn)
         """.stripMargin
      sqlContext.sql(joinSql).registerTempTable(incrTable)

      val tmpSql =
        s"""
           |select distinct mdn,imsicdma,imsilte,iccid,imei,company,companycode as vpdncompanycode,nettype,vpdndomain,isvpdn,subscribetimeaaa,
           |subscribetimehlr,subscribetimehss,subscribetimepcrf,firstactivetime,userstatus,atrbprovince,userprovince, crttime
           |from ${incrTable} t lateral view explode(split(t.vpdncompanycode,',')) c as companycode where mdn is not null
       """.stripMargin

      resultDF = sqlContext.sql(tmpSql)

    } else {
      logInfo("syncType:" + syncType + s" invalid, expect:<${SYCNTYPE_FULL}> or <${SYCNTYPE_INCR}>")
      System.exit(1)
    }



    val vpdnFileLocation = vpdnInput + "/" + vpdnWildcard
    val isvpdnFileExists = if(fileSystem.globStatus(new Path(vpdnFileLocation)).length>0) true else false

    if(isvpdnFileExists){
      import sqlContext.implicits._
      val vpdnProvDF = sqlContext.read.format("text").load(vpdnFileLocation).map(x=>x.getString(0).split("\t")).map(x=> VPDNProv(x(0),x(1))).toDF()
      val vpdnProvTable = "vpdnProvTable"
      vpdnProvDF.registerTempTable(vpdnProvTable)
      sqlContext.cacheTable(vpdnProvTable)
      val resultTable = "resultTable"
      resultDF = resultDF.join(broadcast(vpdnProvDF), Seq("vpdncompanycode"),"left")

    }else{
      logError(s"vpdnFileLocation: ${vpdnFileLocation} not exists.")
    }
   // 先存放在临时目录， 然后mv到分区的目录下面
    resultDF.coalesce(11).write.mode(SaveMode.Overwrite).format("orc").save(outputPath + "/temp/" + dataDayid)
    val dataPath = new Path(outputPath + "data/d=" + dataDayid +"/*")


    fileSystem.globStatus(dataPath).foreach(x=> fileSystem.delete(x.getPath(),false))

    val tmpPath = new Path(outputPath + "/temp/" + dataDayid + "/*.orc")
    val tmpStatus = fileSystem.globStatus(tmpPath)
    var num = 0

    tmpStatus.map(tmpStat => {
      val tmpLocation = tmpStat.getPath().toString  //  temp/2017/1.orc
      var dataLocation = tmpLocation.replace(outputPath + "temp/" + dataDayid, outputPath + "data/d=" + dataDayid + "/") // temp/2017/1.orc   data/d=2017/1.orc
      val index = dataLocation.lastIndexOf("/")
      dataLocation = dataLocation.substring(0, index + 1) + dataDayid + "-" + num + ".orc" // data/2017-1.orc
      num = num + 1

      val tmpPath = new Path(tmpLocation)
      val dataPath = new Path(dataLocation)

      if (!fileSystem.exists(dataPath.getParent)) {
        fileSystem.mkdirs(dataPath.getParent)
      }
      fileSystem.rename(tmpPath, dataPath)

    })


    val sql = s"alter table $userTable add IF NOT EXISTS partition(d='$dataDayid')"
    logInfo(s"partition $sql")
    sqlContext.sql(sql)



  }

}
