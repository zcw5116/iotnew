package com.zyuc.stat.iot.multiana

import com.zyuc.stat.iot.etl.util.CommonETLUtils
import com.zyuc.stat.properties.ConfigProperties
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{Logging, SparkConf, SparkContext}

/**
  * Created by zhoucw on 17-8-17.
  *  modify by limm on 17-09-04
  *  废弃
  */
object OnlineMultiAnalysis extends Logging{

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()//.setAppName("UserOnlineBaseData").setMaster("local[4]")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    val hivedb = ConfigProperties.IOT_HIVE_DATABASE
    sqlContext.sql("use " + hivedb)


    val appName =  sc.getConf.get("spark.app.name")  // name_20170817
    val radiusTable = sc.getConf.get("spark.app.table.radiusTable")  //iot_radius_data_day  // iot_useronline_base_nums
    val userTablePartitionID = sc.getConf.get("spark.app.userTablePartitionID")
    val userTable = sc.getConf.get("spark.app.table.userTable") //"iot_customer_userinfo"
    val basenumTable = sc.getConf.get("spark.app.table.basenumTable", "iot_useronline_base_nums")
    val baseHourTime = sc.getConf.get("spark.app.online.baseHourTime")
    val onlineDayid = sc.getConf.get("spark.app.online.onlineDayid") // yyyyMMddHH
    val outputPath = sc.getConf.get("spark.app.outputPath") // "/hadoop/IOT/data/online/secondaryoutput/"
    val onlineHourTable = sc.getConf.get("spark.app.onlineHourTable")

    val partitionDOfOnline = onlineDayid.substring(2,8)

    val partitionDOfBase = baseHourTime.substring(2, 8)
    val partitionHOfBase = baseHourTime.substring(8, 10)

    if(baseHourTime.substring(0,8)>onlineDayid){
      logError(s"$baseHourTime $onlineDayid , baseHourTime cannot greater than onlineDayidTime")
      return
    }
    val onlineDF = sqlContext.sql(
      s"""select ${partitionDOfOnline} as d, vpdncompanycode,floor(avg(g3cnt)) onlinenum,'3G'nettype
         |from  ${radiusTable}
         |where d = "${partitionDOfBase}"
         |group by vpdncompanycode
       """.stripMargin
    )

//   val radiusByHourSql =
//     s"""
//        |select vpdncompanycode, nettype, substr(time, 3, 6) as d, substr(time, 9, 2) as h,
//        |sum(case when status='Start' then 1 else 0 end ) as startnum,
//        |sum(case when status='Stop' then 1 else 0 end ) as stopnum
//        |from ${radiusTable} where d='${partitionDOfOnline}'
//        |group by vpdncompanycode, nettype, substr(time, 3, 6), substr(time, 9, 2)
//      """.stripMargin

//   val radiusByHourTable = s"radiusByHourTable_$onlineDayid"
//   sqlContext.sql(
//     s"""CACHE TABLE ${radiusByHourTable} as
//        |select vpdncompanycode, nettype, h, (startnum- stopnum) as onlinenum
//        |from ( $radiusByHourSql ) t

//      """.stripMargin)

//   val baseOnlineTable = s"baseOnlineTable_$onlineDayid"
//   sqlContext.sql(
//     s"""CACHE TABLE ${baseOnlineTable} as
//        |select vpdncompanycode, '3G' nettype, g3cnt as onlinenum
//        |from $basenumTable where d='${partitionDOfBase}' and h='${partitionHOfBase}'
//        |union all
//        |select vpdncompanycode, '4G' nettype, pgwcnt as onlinenum
//        |from $basenumTable where d='${partitionDOfBase}' and h='${partitionHOfBase}'
//      """.stripMargin)


//   val initHourid="00"
//   var resultDF:DataFrame = null
//   for(i <- 0 until 24){
//     val hourid = DateUtils.timeCalcWithFormatConvertSafe(initHourid, "HH", i*3600, "HH")

//     val onlineSql =
//       s"""select ${partitionDOfOnline} as d, '${hourid}' as hourid, vpdncompanycode, nettype, floor(sum
//          |(onlinenum)) as onlinenum
//          |from
//          |(
//          |    select vpdncompanycode, nettype, onlinenum
//          |    from ${radiusByHourTable}
//          |    where h<=${hourid}
//          |    union all
//          |    select vpdncompanycode,nettype, onlinenum
//          |    from ${baseOnlineTable}
//          |) t
//          |group by vpdncompanycode, nettype
//        """.stripMargin
//     val tmpDF = sqlContext.sql(onlineSql)
//     if(i==0){
//       resultDF=tmpDF
//     }else{
//       resultDF = resultDF.unionAll(tmpDF)
//     }

//   }
    var resultDF = null

    val partitions="d"
    val fileSystem = FileSystem.newInstance(sc.hadoopConfiguration)
    val coalesceNum = 1

    CommonETLUtils.saveDFtoPartition(sqlContext, fileSystem, resultDF, coalesceNum, partitions, onlineDayid, outputPath, onlineHourTable, appName)


  }

}
