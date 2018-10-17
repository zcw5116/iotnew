package com.zyuc.stat.nbiot.etl

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zhoucw on 18-5-11 下午8:41.
  */
object CustUserBaseOnCRM {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()//.setAppName("test").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)

    val datatime = sc.getConf.get("spark.app.datatime", "20180510")
    val outputPath = sc.getConf.get("spark.app.outputpath", "/user/iot/data/baseuser/data/")
    val inputPath = sc.getConf.get("spark.app.inputPath", "/user/iot/data/CRM/data/20180916")

    val crmDF = sqlContext.read.format("orc").load(inputPath)
    crmDF.filter("STAT!='%拆机%'")
      .selectExpr("concat('86',MDN) as mdn","CUST_ID as custid","PROD_TYPE as prodtype",
        "BELO_PROV_COMP as beloprov", "BELO_CITY_COMP as belocity",
        "IF_NB as isnb","IF_4G as is4g","IF_3G as is3g","IF_2G as is2g")
        .repartition(10).write.format("orc").mode(SaveMode.Overwrite).save(outputPath + "d=" + datatime)


  }

}
