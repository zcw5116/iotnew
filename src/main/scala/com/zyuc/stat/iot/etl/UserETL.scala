package com.zyuc.stat.iot.etl

import com.zyuc.stat.iot.etl.util.UserInfoConverterUtils
import com.zyuc.stat.properties.ConfigProperties
import com.zyuc.stat.utils.DateUtils
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

import scala.collection.mutable

/**
  * Created by zhoucw on 17-9-20.
  * @deprecated
  */
object UserETL {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
    .setMaster("local[4]").setAppName("fd")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    sqlContext.sql("use " + ConfigProperties.IOT_HIVE_DATABASE)
    val appName = sc.getConf.get("spark.app.appName", "UserInfoETL")
    val dataDayid = sc.getConf.get("spark.app.dataDayid", "20170919")
    // val dataDayid = "20170714"
    val userTable = sc.getConf.get("spark.app.userTable", "iot_customer_userinfo")
    // val userTable = "iot_customer_userinfo"
    val syncType = sc.getConf.get("spark.app.syncType", "incr")
    val inputPath = "/tmp/user/" //sc.getConf.get("spark.app.inputPath")
    //val inputPath = "/hadoop/IOT/ANALY_PLATFORM/BasicData/UserInfo/"
    val outputPath = sc.getConf.get("spark.app.outputPath", "/tmp/output/")
    //val outputPath = "/hadoop/IOT/ANALY_PLATFORM/BasicData/output/UserInfo/"
    val fileWildcard = "incr_userinfo_qureyes*" // sc.getConf.get("spark.app.fileWildcard")
    // val fileWildcard = "all_userinfo_qureyes_20170714*"
    // val fileWildcard = "incr_userinfo_qureyes_20170715*"
    val fileSystem = FileSystem.get(sc.hadoopConfiguration)
    val fileLocation = inputPath + "/" + fileWildcard
    val crttime = DateUtils.getNowTime("yyyy-MM-dd HH:mm:ss")

    val textDF = sqlContext.read.format("text").load(fileLocation)
    textDF.map(x=>x.getString(0)).flatMap(line=>{
      val p = line.split("\\|",24)
      val vpdnCompanyList = p(6).split(",")
      val domainList = p(9).split(",")
      val vpdnAndDomain = vpdnCompanyList.zip(domainList)
      val set = new mutable.HashSet[Tuple19[String, String, String,String, String, String, String, String,String, String, String, String, String,String,String, String, String, String, String]]
      vpdnAndDomain.foreach(e=>set.+=((p(0), p(1), p(2), p(3), p(4), p(5), e._1, p(7), p(8), e._2, p(10), p(16), p(17), p(18), p(19), p(20), p(21), p(22), p(23))))
      set
    })


    sc.stop()

  }
}
