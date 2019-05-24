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
    val d = dataTime.substring(0, 8)
    val dd = dataTime.substring(2, 8)
    val partitionPath = s"/d=$dd/h=*/m5=*"

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
         |on(a.Called_Number=b.mdn_called)
         |left join ${userTable} c
         |on(a.Calling_Number=c.mdn_calling)
       """.stripMargin)

    df1.repartition(10).write.format("orc").mode(SaveMode.Overwrite).save(outputPath + "/" + d + "/base")

    val baseTable = "baseTable"
    sqlContext.read.format("orc").load(outputPath + "/" + d + "/base").registerTempTable(baseTable)

    //-----发送custid  mdn  count(distinct mdn)    接收custid  mdn  count(distinct mdn)
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
         |where custid_calling is not null and custid_called is null
       """.stripMargin)

    val calledDF = sqlContext.sql(
      s"""
         |select custid_called as custid, '日' as anaCycle, ${d} as summ_cycle,
         |        '0' as calling_cnt, called_cnt, called_cnt as call_cnt, cnt2 as cnt
         |from ${cntTable}
         |where custid_called is not null and custid_calling is null
       """.stripMargin)

    val callDF = sqlContext.sql(
      s"""
         |select custid_called as custid, '日' as anaCycle, ${d} as summ_cycle,
         |        calling_cnt, called_cnt, (calling_cnt + called_cnt) as call_cnt, (cnt1 + cnt2) as cnt
         |from ${cntTable}
         |where custid_calling is not null and custid_called is not null
       """.stripMargin)
    //基表保存到hdfs
    callingDF.unionAll(calledDF).unionAll(callDF).repartition(10).write.format("orc").mode(SaveMode.Overwrite)
      .save(outputPath + "/" + d + "/baseMsg")


    //基表部分字段注册成临时表
    val baseMsgTable = "baseMsgTable"
    sqlContext.read.format("orc").load(outputPath + "/" + d + "/baseMsg").filter("cnt>100")
      .selectExpr("custid", "call_cnt as TOTAL_cnt").registerTempTable(baseMsgTable)


    val abnormalCalled_table = "abnormalCalled_table"
    sqlContext.sql(
            s"""
               |select custid_called as custid_call, Called_Number as mdn, count(*) as called_cnt, TOTAL_cnt
               |from ${baseTable} u
               |inner join ${baseMsgTable} b on u.custid_called=b.custid
               |group by custid_called, Called_Number, TOTAL_cnt
             """.stripMargin).registerTempTable(abnormalCalled_table)

    val abnormalCalling_table = "abnormalCalling_table"
    sqlContext.sql(
            s"""
               |select custid_calling as custid_call, Calling_Number as mdn, count(*) as calling_cnt, TOTAL_cnt
               |from ${baseTable} u
               |inner join ${baseMsgTable} b on u.custid_calling=b.custid
               |group by custid_calling, Calling_Number, TOTAL_cnt
             """.stripMargin).registerTempTable(abnormalCalling_table)

    //上面两个合并on cust+mdn   平均短信数判断异常卡？
    val abnormalDF = sqlContext.sql(
      s"""
         |select a.custid_call, a.mdn, b.calling_cnt, a.called_cnt, (b.calling_cnt + a.called_cnt) as calls_cnt, a.TOTAL_cnt
         |from $abnormalCalled_table a
         |inner join $abnormalCalling_table b
         |on(a.custid_call=b.custid_call and a.mdn=b.mdn)
       """.stripMargin).filter("calls_cnt*100/TOTAL_cnt<50 or calls_cnt*100/TOTAL_cnt>150")

    val abnormalMsg = abnormalDF.selectExpr("custid_call as custid", s"'${d}' as summ_cycle", "mdn",
      "calling_cnt", "called_cnt", "calls_cnt",
      "'-1' as upPacket", "'-1' as downPacket", "'-1' as totalPacket")
    //异常卡短信保存到hdfs
    abnormalMsg.repartition(10).write.format("orc").mode(SaveMode.Overwrite).save(outputPath + "/" + d + "/abnormalMsg")



  }
}
