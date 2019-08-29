package com.zyuc.stat.nbiot.etl

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by liuzk on 18-11-02.
  */
object CRMETL2_YiYang_csv {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    val sc = new SparkContext(sparkConf)
    val hiveContext = new HiveContext(sc)
    //val sqlContext = new HiveContext(sc)
    val appName = sc.getConf.get("spark.app.name")
    val inputPath = sc.getConf.get("spark.app.inputpath", "/user/iot/data/metadata/CRM")
    val outputPath = sc.getConf.get("spark.app.outputpath", "/user/iot/tmp/CRM2yiyang")
    val outputPathTmp = sc.getConf.get("spark.app.outputPathTmp", "/user/iot/tmp/CRM2yiyang_tmp")
    val csvNum = sc.getConf.get("spark.app.csvNum", "20").toInt

    val fileSystem = FileSystem.get(sc.hadoopConfiguration)
    val dataTime = appName.substring(appName.lastIndexOf("_") + 1)

    val inputfiles = inputPath + "/JiTuanWangYun-DuanDaoDuanBaoZhangXiTong_" + dataTime + ".txt"
    val rdd = sc.textFile(inputfiles).map(x => x.split("\t", -1))
      .map(x => Row(x(0), x(1), x(2), x(3), x(4), x(5), x(6), x(7), x(8), x(9),
      x(10), x(11), x(12), x(13), x(14), x(15), x(16), x(17), x(18), x(19),
      x(20), x(21), x(22), x(23), x(24), x(25), x(26), x(27), x(28), x(29), x(30), x(31), x(32), x(33), x(34), x(35)))


    val df = hiveContext.createDataFrame(rdd, struct)

    val resultDF = df.repartition(csvNum)
      .selectExpr("MDN", "ProductID", "CUST_ID", "CUST_BELO_ENTE","BELO_CITY_COMP",
        "BELO_PROV_COMP", "IND_TYPE","IND_DET_TYPE", "PROD_TYPE", "ActiveTime",
        "CmpTime", "STAT", "IMSI_3G", "IMSI_4G",
        "IF_2G", "IF_3G", "IF_4G", "IF_NB", "AccountManager")

    resultDF.coalesce(1).write.format("com.databricks.spark.csv").option("header","true").mode(SaveMode.Overwrite).save(outputPath)


  }
    val struct = StructType(Array(
    StructField("USERKEY", StringType),

    StructField("MDN", StringType),
    StructField("ProductID", StringType),
    StructField("CUST_ID", StringType),
    StructField("CUST_BELO_ENTE", StringType),
    StructField("BELO_CITY_COMP", StringType),
    StructField("BELO_PROV_COMP", StringType),
    StructField("IND_TYPE", StringType),

    StructField("IND_DET_TYPE", StringType),
    StructField("PROD_TYPE", StringType),
    StructField("ActiveTime", StringType),
    StructField("CmpTime", StringType),
    StructField("STAT", StringType),
    StructField("IMSI_3G", StringType),
    StructField("IMSI_4G", StringType),

    StructField("ICCID", StringType),
    StructField("IF_VPDN", StringType),
    StructField("IF_DRTSERV", StringType),
    StructField("IF_2G", StringType),
    StructField("IF_3G", StringType),
    StructField("IF_4G", StringType),
    StructField("IF_FLUX", StringType),

    StructField("IF_RegLimit", StringType),
    StructField("IF_VPDN_CN2", StringType),
    StructField("IF_MCB", StringType),
    StructField("RegLimitProv", StringType),
    StructField("IF_Rcv_SM", StringType),
    StructField("IF_Send_SM", StringType),
    StructField("IF_P2P_SM", StringType),

    StructField("DRTIP", StringType),
    StructField("Domain", StringType),
    StructField("APN", StringType),
    StructField("IF_VPDNAAA", StringType),

    StructField("IF_NB", StringType),
    StructField("AccountManager", StringType),
    StructField("amcustid", StringType)
  ))

}
