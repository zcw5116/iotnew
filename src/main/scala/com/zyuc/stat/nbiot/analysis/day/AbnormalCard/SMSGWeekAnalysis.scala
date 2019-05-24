package com.zyuc.stat.nbiot.analysis.day.AbnormalCard

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by liuzk on 19-1-7.
  *   msg 周统计
  */
object SMSGWeekAnalysis {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()//.setMaster("local[2]").setAppName("name_20181214")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    val appName = sc.getConf.get("spark.app.name")
    val inputPath = sc.getConf.get("spark.app.inputPath", "/user/iot/data/cdr/abnormalCard/summ_d/msg/")
    val outputPath = sc.getConf.get("spark.app.outputPath","/user/iot/data/cdr/abnormalCard/summ_w/msg")
//    val userPath = sc.getConf.get("spark.app.userPath", "/user/iot/data/baseuser/data/")
//    val userDataTime = sc.getConf.get("spark.app.userDataTime", "20180510")

    val onedayago = sc.getConf.get("spark.app.onedayago","20190120")
    val twodayago = sc.getConf.get("spark.app.twodayago","20190119")
    val threedayago = sc.getConf.get("spark.app.threedayago","20190118")
    val fourdayago = sc.getConf.get("spark.app.fourdayago","20190117")
    val fivedayago = sc.getConf.get("spark.app.fivedayago","2190116")
    val sixdayago = sc.getConf.get("spark.app.sixdayago","20190115")
    val sevendayago = sc.getConf.get("spark.app.sevendayago","20190114")

    val fileSystem = FileSystem.get(sc.hadoopConfiguration)
    val dataTime = appName.substring(appName.lastIndexOf("_") + 1)
    val d = dataTime.substring(0, 8)


    val df1 = sqlContext.read.format("orc").load(inputPath + onedayago + "/base",
      inputPath + twodayago + "/base",inputPath + threedayago + "/base",inputPath + fourdayago + "/base",
      inputPath + fivedayago + "/base",inputPath + sixdayago + "/base",inputPath + sevendayago + "/base")

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
         |select custid_calling as custid, '周' as anaCycle, ${d} as summ_cycle,
         |        calling_cnt, '0' as called_cnt, calling_cnt as call_cnt, cnt1 as cnt
         |from ${cntTable}
         |where custid_calling is not null and custid_called is null
       """.stripMargin)

    val calledDF = sqlContext.sql(
      s"""
         |select custid_called as custid, '周' as anaCycle, ${d} as summ_cycle,
         |        '0' as calling_cnt, called_cnt, called_cnt as call_cnt, cnt2 as cnt
         |from ${cntTable}
         |where custid_called is not null and custid_calling is null
       """.stripMargin)

    val callDF = sqlContext.sql(
      s"""
         |select custid_called as custid, '周' as anaCycle, ${d} as summ_cycle,
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
