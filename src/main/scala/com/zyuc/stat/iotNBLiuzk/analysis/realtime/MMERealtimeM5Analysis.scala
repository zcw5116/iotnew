package com.zyuc.stat.iotNBLiuzk.analysis.realtime

import com.zyuc.stat.properties.ConfigProperties
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by liuzk on 18-5-10.
  */
object MMERealtimeM5Analysis {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)

    val appName = sc.getConf.get("spark.app.name","name_MMERealtimeM5Analysis")
    val datatime = sc.getConf.get("spark.app.datatime","201805101400")
    val inputPath = "hdfs://spark1234:8020/user/epciot/data/mme/transform/nb/data"
    val outputPath = "hdfs://spark1234:8020/user/epciot/data/mme/analy_realtime/nb"
    //sqlContext.sql("use " + ConfigProperties.IOT_HIVE_DATABASE)

    val d = datatime.substring(2, 8)
    val h = datatime.substring(8, 10)
    val m5 = datatime.substring(10, 12)
    val partitionPath = s"/d=$d/h=$h/m5=$m5"
    val dd = datatime.substring(0, 8)
    val hm5 = datatime.substring(8, 12)

    sqlContext.read.format("orc").load("/user/epciot/data/basic/AllUserInfo/").registerTempTable("AllUserTable")
    sqlContext.read.format("orc").load(inputPath + partitionPath).registerTempTable("MMETempTable")
    sqlContext.read.format("orc").load("/user/epciot/data/basic/IotBSInfo/data/").registerTempTable("IOTBSInfo")

    val sql =
      s"""
         |select '${datatime}' as gather_cycle, '${dd}' as gather_date, '${hm5}' as gather_time,
         |a.cust_id, i.provname as province, i.cityname as city,
         |count(*) as REQS ,
         |sum(case when m.result='failed' then 1 else 0 end) as FAILREQS ,
         |count(distinct (case when m.result = 'failed' then m.MSISDN else '' end)) as FAILUSERS
         |from
         |MMETempTable m
         |left join
         |IOTBSInfo i
         |on m.enbid=i.enbid and m.province=i.provname
         |inner join
         |AllUserTable a
         |on a.mdn=m.msisdn
         |group by a.cust_id, i.provname, i.cityname
         |GROUPING SETS(a.cust_id,(a.cust_id, i.provname),(a.cust_id, i.provname, i.cityname))
       """.stripMargin

    sqlContext.sql(sql)
      .repartition(20).write.mode(SaveMode.Overwrite).format("orc")
      .save(outputPath + partitionPath)

    //---

  }

}
