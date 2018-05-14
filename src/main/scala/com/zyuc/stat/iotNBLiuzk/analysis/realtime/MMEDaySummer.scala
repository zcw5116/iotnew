package com.zyuc.stat.iotNBLiuzk.analysis.realtime

import com.zyuc.stat.properties.ConfigProperties
import org.apache.spark.sql.SaveMode
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by liuzk on 18-5-11.
  */
object MMEDaySummer {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)

    val appName = sc.getConf.get("spark.app.name","name_201805101400")
    val datatime = sc.getConf.get("spark.app.datatime","201805101400")
    val inputPath = "hdfs://inmnmbd02:8020/user/epciot/data/mme/transform/nb/data"
    val outputPath = "hdfs://inmnmbd02:8020/user/epciot/data/mme/summ_d/nb"
    sqlContext.sql("use " + ConfigProperties.IOT_HIVE_DATABASE)

    val d = datatime.substring(2, 8)
    val dd = datatime.substring(0, 8)
    //val h = loadTime.substring(8, 10)
    //val m5 = loadTime.substring(10, 12)
    val partitionPath = s"/d=$d/h=*/m5=*"

    sqlContext.read.format("orc").load(inputPath + partitionPath).registerTempTable("MMETempTable")
    sqlContext.read.format("orc").load("/user/epciot/data/basic/IotBSInfo/data/").registerTempTable("IOTBSInfo")
    sqlContext.read.format("orc").load("/user/epciot/data/basic/AllUserInfo/").registerTempTable("AllUserTable")

    val sql =
      s"""
         |select '${dd}' as summ_cycle, a.cust_id, i.provname as province, i.cityname as city, i.enbid, m.T13,
         |count(*) as REQS ,
         |sum(case when m.result='failed' then 1 else 0 end) as FAILREQS ,
         |count(distinct (case when m.result = 'failed' then MSISDN else '' end)) as FAILUSERS
         |from
         |MMETempTable m
         |inner join
         |IOTBSInfo i
         |on m.enbid=i.enbid
         |inner join
         |AllUserTable a
         |on a.mdn=m.msisdn
         |group by a.cust_id, i.provname, i.cityname, i.enbid, m.T13
         |GROUPING SETS(a.cust_id,(a.cust_id, i.provname),(a.cust_id, i.provname, i.cityname ),
         |(a.cust_id, i.provname, i.cityname, i.enbid),(a.cust_id, i.provname, i.cityname, i.enbid, m.T13))
       """.stripMargin

    sqlContext.sql(sql)
      .repartition(20).write.mode(SaveMode.Overwrite).format("orc")
      .save(outputPath + partitionPath)

  }

}
