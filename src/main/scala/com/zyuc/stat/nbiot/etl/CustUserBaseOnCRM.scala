package com.zyuc.stat.nbiot.etl

import com.zyuc.stat.utils.FileUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by liuzk on 18-10-11 下午8:41.
  *
  * 每天将三天前的 CRM  数据更新  orc
  */
object CustUserBaseOnCRM {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()//.setAppName("test").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    val appName = sc.getConf.get("spark.app.name")
    val datatime = sc.getConf.get("spark.app.datatime", "20180510")
    val outputPath = sc.getConf.get("spark.app.outputpath", "/user/iot/data/baseuser/data/")
    val inputPath = sc.getConf.get("spark.app.inputPath", "/user/iot/data/metadata/CRM/")
    val fileSystem = FileSystem.get(sc.hadoopConfiguration)

    val CRMTime = appName.substring(appName.lastIndexOf("_") + 1)
    val CRMpath = inputPath + s"/JiTuanWangYun-DuanDaoDuanBaoZhangXiTong_${CRMTime}.txt"
    val path = new Path(CRMpath)
    println("----------------")
    println(path.toString)
    if (FileUtils.getFilesByWildcard(fileSystem, path.toString).length > 0) {
      import sqlContext.implicits._
      val df = sc.textFile(CRMpath).map(x => x.split("\t", 35)).filter(_.length !=1)// 34-->35
        .map(x => (x(12),x(1), x(3),x(5), x(6),x(7),x(8),x(9),x(33), x(20), x(19),x(18),x(13),x(14)))
        .toDF("STAT","MDN","custid","belocity","beloprov","ind_type","ind_det_type","prodtype",
          "isnb","is4g","is3g","is2g","IMSI_3G","IMSI_4G")

      df.filter("STAT!='%拆机%'")
        .selectExpr("concat('86',MDN) as mdn","custid","prodtype","ind_det_type","ind_type",//新增九大行业
          "regexp_replace(beloprov, '省|市|壮族|自治区|维吾尔|回族', '') as beloprov",
          "regexp_replace(belocity, '电信', '') as belocity",
          "isnb","is4g","is3g","is2g","IMSI_3G","IMSI_4G")
        .coalesce(20).write.format("orc").mode(SaveMode.Overwrite).save(outputPath + "tmpToDay")

      // 删除目录下的文件
      val baseUser = new Path(outputPath + "d=" + datatime + "/*.orc")
      fileSystem.globStatus(baseUser).foreach(x => fileSystem.delete(x.getPath(), false))
      // 移动临时目录到文件到正式的目录
      val tmpPath = new Path(outputPath + "tmpToDay/*.orc")
      fileSystem.globStatus(tmpPath).foreach(x => {
        val tmpLocation = x.getPath().toString
        var dataLocation = tmpLocation.replace(outputPath + "tmpToDay/", outputPath + "d=" + datatime + "/")

        val tmpPath = new Path(tmpLocation)
        val dataPath = new Path(dataLocation)

        if (!fileSystem.exists(dataPath.getParent)) {
          fileSystem.mkdirs(dataPath.getParent)
        }
        fileSystem.rename(tmpPath, dataPath)
      })
    }

    /*val crmDF = sqlContext.read.format("orc").load(inputPath)
    crmDF.filter("STAT!='%拆机%'")
      .selectExpr("concat('86',MDN) as mdn","CUST_ID as custid","PROD_TYPE as prodtype",
        "BELO_PROV_COMP as beloprov", "BELO_CITY_COMP as belocity",
        "IF_NB as isnb","IF_4G as is4g","IF_3G as is3g","IF_2G as is2g")
        .repartition(10).write.format("orc").mode(SaveMode.Overwrite).save(outputPath + "d=" + datatime)*/


  }

}
