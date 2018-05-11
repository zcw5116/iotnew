package com.zyuc.stat.iotNBLiuzk.analysis.realtime

import org.apache.spark.sql.SaveMode
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by liuzk on 18-5-11.
  *
  * 解析全量用户数据只取 mdn , cust_id
  */
object mytest {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)

    import sqlContext.implicits._
    val appName = sc.getConf.get("spark.app.name","name_CDRRealtimeM5Analysis")
    sc.textFile("file:///slview/nms/python/jars/all_userinfo_qureyes_20180510.txt")
      .map(line=>line.split("\\|",26)).map(x=>(x(0),x(25))).toDF("mdn","cust_id")
      .registerTempTable("AllUserTable")

    sqlContext.sql("select * from AllUserTable")
      .repartition(20).write.mode(SaveMode.Overwrite).format("orc")
      .save("/user/epciot/data/basic/AllUserInfo")
  }

}
