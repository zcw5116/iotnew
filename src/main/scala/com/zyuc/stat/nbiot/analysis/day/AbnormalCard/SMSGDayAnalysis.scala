package com.zyuc.stat.nbiot.analysis.day.AbnormalCard

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by liuzk on 19-1-7.
  */
object SMSGDayAnalysis {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()//.setMaster("local[2]").setAppName("name_20181214")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    val appName = sc.getConf.get("spark.app.name")
    val inputPath = sc.getConf.get("spark.app.inputPath", "/user/iot/data/msg/transform/nb/data")
    val outputPath = sc.getConf.get("spark.app.outputPath","/user/iot/data/cdr/abnormalCard/summ_d/msg")
    val userPath = sc.getConf.get("spark.app.userPath", "/user/iot/data/baseuser/data/")
    val userDataTime = sc.getConf.get("spark.app.userDataTime", "20180510")

    val fileSystem = FileSystem.get(sc.hadoopConfiguration)
    val dataTime = appName.substring(appName.lastIndexOf("_") + 1)
    val d = dataTime.substring(2, 8)
    val partitionPath = s"/d=$d/h=*/m5=*"

    val cdrTempTable = "CDRTempTable"
    sqlContext.read.format("orc").load(inputPath + partitionPath)
      .filter("Result_of_Last_Send='成功'").selectExpr("Called_Number","Calling_Number")
      .registerTempTable(cdrTempTable)

    val userDataPath = userPath + "/d=" + userDataTime
    val userDF = sqlContext.read.format("orc").load(userDataPath).filter("beloprov='江苏'").selectExpr("mdn","custid")
    val tmpUserTable = "spark_tmpUser"
    userDF.registerTempTable(tmpUserTable)

    val userTable = "spark_User"
    sqlContext.sql(
      s"""
         |cache table ${userTable}
         |as
         |select mdn as mdn_called, mdn as mdn_calling, custid
         |from ${tmpUserTable}
       """.stripMargin)


    //------------mdn先关联custid，保存到hdfs在分析
    val df1 = sqlContext.sql(
      s"""
         |select a.Called_Number, b.custid as custid_called, a.Calling_Number, c.custid as custid_calling
         |from
         |${cdrTempTable} a
         |left join ${userTable} b
         |on a.Called_Number=b.mdn_called
         |left join ${userTable} c
         |on a.Calling_Number=c.mdn_calling
       """.stripMargin)
    println("---------df1-----------")
    df1.show
    val df2 = df1.filter("custid_called is not null and custid_calling is not null")
    println("---------df2-----------")
    df2.show
    println("---------df2end-----------")
    df2.write.format("orc").mode(SaveMode.Overwrite).save(outputPath + "/" + d + "/base")

    val baseTable = "baseTable"
    sqlContext.read.format("orc").load(outputPath + "/" + d + "/base").registerTempTable(baseTable)

    //------------按custid分组，分别统计接收 发送数，mdn数
    val cntTable = "cntTable"
    sqlContext.sql(
              s"""
                 |select *
                 |from
                 |(select custid_calling, count(*) as calling_cnt, count(distinct Calling_Number) as cnt1
                 |from ${baseTable}
                 |group by custid_calling
                 |) a
                 |full outer join
                 |(select custid_called, count(*) as called_cnt, count(distinct Called_Number) as cnt2
                 |from ${baseTable}
                 |group by custid_called
                 |) b
                 |on(a.custid_calling=b.custid_called)
               """.stripMargin).registerTempTable(cntTable)

    val callingDF = sqlContext.sql(
      s"""
         |select custid_calling as custid, '日' as anaCycle, ${d} as summ_cycle,
         |        calling_cnt, '0' as called_cnt, calling_cnt as call_cnt, cnt1 as cnt
         |from ${cntTable}
         |where custid_calling is not null
       """.stripMargin)

    val calledDF = sqlContext.sql(
      s"""
         |select custid_called as custid, '日' as anaCycle, ${d} as summ_cycle,
         |        '0' as calling_cnt, called_cnt, called_cnt as call_cnt, cnt2 as cnt
         |from ${cntTable}
         |where custid_called is not null
       """.stripMargin)

    val callDF = sqlContext.sql(
      s"""
         |select custid_called as custid, '日' as anaCycle, ${d} as summ_cycle,
         |        calling_cnt, called_cnt, (calling_cnt + called_cnt) as call_cnt, (cnt1 + cnt2) as cnt
         |from ${cntTable}
         |where custid_calling is not null and custid_called is not null
       """.stripMargin)
    //基表保存到hdfs
    callingDF.unionAll(calledDF).unionAll(callDF).write.format("orc").mode(SaveMode.Overwrite)
      .save(outputPath + "/" + d + "/baseMsg")


    //基表部分字段注册成临时表
    val baseMsgTable = "baseMsgTable"
    sqlContext.read.format("orc").load(outputPath + "/" + d + "/baseMsg").filter("cnt>100")
      .selectExpr("custid", "cnt").registerTempTable(baseMsgTable)

    val abnormalCalled = sqlContext.sql(
                        s"""
                           |select custid_called as custid_call, Called_Number as mdn, count(*) as called_cnt, '0' as calling_cnt,
                           |       count(*) as call_cnt, cnt
                           |from ${baseTable} u
                           |left join ${baseMsgTable} b on u.custid_called=b.custid
                           |group by custid_called, Called_Number, cnt
                         """.stripMargin).filter("call_cnt*100/cnt<50 or call_cnt*100/cnt>150")

    val abnormalCalling = sqlContext.sql(
                        s"""
                           |select custid_calling as custid_call, Calling_Number as mdn, '0' as called_cnt, count(*) as calling_cnt,
                           |       count(*) as call_cnt, cnt
                           |from ${baseTable} u
                           |left join ${baseMsgTable} b on u.custid_calling=b.custid
                           |group by custid_calling, Calling_Number, cnt
                         """.stripMargin).filter("call_cnt*100/cnt<50 or call_cnt*100/cnt>150")

    val abnormalCallDF = abnormalCalled.unionAll(abnormalCalling).selectExpr("custid_call as custid", s"${d} as summ_cycle", "mdn",
      "calling_cnt", "called_cnt", "call_cnt",
      "'0' as upPacket", "'0' as downPacket", "'0' as totalPacket")

    //异常卡短信保存到hdfs
    abnormalCallDF.write.format("orc").mode(SaveMode.Overwrite).save(outputPath + "/" + d + "/abnormalMsg")


  }
}
