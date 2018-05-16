package com.zyuc.stat.iotNBLiuzk.etl

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zhoucw on 18-5-11 下午8:41.
  */
object CustUser {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()//.setAppName("test").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)

    val nbpara = sc.getConf.get("spark.app.nbpara", "/user/epciot/data/metadata/nbpara/nbpara.csv")
    val datatime = sc.getConf.get("spark.app.datatime", "20180510")
    val inputPath = sc.getConf.get("spark.app.inputpath", "/user/epciot/data/metadata/nb_staticpara")
    val outputPath = sc.getConf.get("spark.app.outputpath", "/user/epciot/data/baseuser/data/")

    val inputfiles = inputPath + "/all_userinfo_qureyes_" + datatime + ".txt"

    // 广播小表,小表数据不能超过10M, 否则需要修改参数
    import sqlContext.implicits._
    val nbparaDF = sc.textFile(nbpara).map(x=>x.trim).toDF("para")
    val tmpParaTable = "tmpnbpara"
    nbparaDF.registerTempTable(tmpParaTable)
    val nbparaTable = "nbpara"
    sqlContext.sql(
      s"""
         |cache table ${nbparaTable}
         |as
         |select para from ${tmpParaTable}
       """.stripMargin)

    sqlContext.sql(s"select * from $nbparaTable").show()


    // 加载用户表
    val userInputDF = sc.textFile(inputfiles).map(x=>x.split("\\|")).filter(_.length==26).
      map(x=>(x(0), x(1), x(2), x(3), x(19), x(20), x(21), x(22), x(23), x(24), x(25))).
      toDF("mdn", "cdmaimsi", "lteimsi", "imei", "belocity", "beloprov",
        "stat", "custtype", "prodtype", "vpdnblock", "custid")
    val usertmpTable = "usertmp"
    userInputDF.registerTempTable(usertmpTable)
    sqlContext.sql("select mdn, substr(u.mdn, 3, 9) from usertmp u").show()

    val userDF = sqlContext.sql(
      s"""
         |select u.*, case when n.para is null then 0 else 1 end isnb
         |from ${usertmpTable} u left join ${nbparaTable} n
         |on( substr(u.mdn, 3, 9) = n.para)
       """.stripMargin)

    userDF.repartition(10).write.format("orc").save(outputPath + "d=" + datatime)
  }

}
