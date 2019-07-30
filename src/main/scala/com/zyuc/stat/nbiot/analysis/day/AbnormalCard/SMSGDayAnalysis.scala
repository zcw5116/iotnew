package com.zyuc.stat.nbiot.analysis.day.AbnormalCard

import com.zyuc.iot.utils.DbUtils
import com.zyuc.stat.nbiot.analysis.realtime.utils.CommonUtils
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

    val tableName = sc.getConf.get("spark.app.tableName","iot_ana_day_abnormal_user")

    val cdrTempTable = "CDRTempTable"
    sqlContext.read.format("orc").load(inputPath + partitionPath)
      .filter("Result_of_Last_Send='成功'").selectExpr("Called_Number","Calling_Number")
      .registerTempTable(cdrTempTable)

    val userDataPath = userPath + "/d=" + userDataTime
    val userDF = sqlContext.read.format("orc").load(userDataPath).filter("beloprov='江苏'")
      .selectExpr("mdn", "custid", "custname", "beloprov", "belocity")

    val tmpUserTable = "spark_tmpUser"
    userDF.registerTempTable(tmpUserTable)

    val userTable = "spark_User"
    sqlContext.sql(
      s"""
         |cache table ${userTable}
         |as
         |select mdn as mdn_called, mdn as mdn_calling, custid, custname, beloprov, belocity
         |from ${tmpUserTable}
       """.stripMargin)


    //------------mdn先关联custid，保存到hdfs在分析
    val df1 = sqlContext.sql(
      s"""
         |select a.Called_Number, b.custid as custid_called, b.custname as custnameed, b.beloprov as beloproved, b.belocity as belocityed,
         |       a.Calling_Number, c.custid as custid_calling, c.custname as custnameing, c.beloprov as beloproving, c.belocity as belocitying
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
                 |(select custid_calling, custnameing, beloproving, belocitying, count(*) as calling_cnt, count(distinct Calling_Number) as cnt1
                 |from ${baseTable}
                 |group by custid_calling, custnameing, beloproving, belocitying
                 |) a
                 |full outer join
                 |(select custid_called, custnameed, beloproved, belocityed, count(*) as called_cnt, count(distinct Called_Number) as cnt2
                 |from ${baseTable}
                 |group by custid_called, custnameed, beloproved, belocityed
                 |) b
                 |on(a.custid_calling=b.custid_called and a.beloproving=b.beloproved and a.belocitying=b.belocityed)
               """.stripMargin).registerTempTable(cntTable)

    val callingDF = sqlContext.sql(
      s"""
         |select custid_calling as custid, custnameing as custname, beloproving as beloprov, belocitying as belocity, '日' as anaCycle, ${d} as summ_cycle,
         |        calling_cnt, '0' as called_cnt, calling_cnt as call_cnt, cnt1 as cnt
         |from ${cntTable}
         |where custid_calling is not null and custid_called is null
       """.stripMargin)

    val calledDF = sqlContext.sql(
      s"""
         |select custid_called as custid, custnameed as custname, beloproved as beloprov, belocityed as belocity, '日' as anaCycle, ${d} as summ_cycle,
         |        '0' as calling_cnt, called_cnt, called_cnt as call_cnt, cnt2 as cnt
         |from ${cntTable}
         |where custid_called is not null and custid_calling is null
       """.stripMargin)

    val callDF = sqlContext.sql(
      s"""
         |select custid_called as custid, custnameed as custname,  beloproving as beloprov, belocitying as belocity, '日' as anaCycle, ${d} as summ_cycle,
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
      .selectExpr("custid", "custname", "beloprov", "belocity", "call_cnt as TOTAL_cnt").registerTempTable(baseMsgTable)


    val abnormalCalled_table = "abnormalCalled_table"
    sqlContext.sql(
            s"""
               |select custid_called as custid_call, u.custnameed, u.beloproved, u.belocityed, Called_Number as mdn, count(*) as called_cnt, TOTAL_cnt
               |from ${baseTable} u
               |inner join ${baseMsgTable} b on (u.custid_called=b.custid and u.beloproved=b.beloprov and u.belocityed=b.belocity)
               |group by custid_called, u.custnameed, u.beloproved, u.belocityed, Called_Number, TOTAL_cnt
             """.stripMargin).registerTempTable(abnormalCalled_table)

    val abnormalCalling_table = "abnormalCalling_table"
    sqlContext.sql(
            s"""
               |select custid_calling as custid_call, u.custnameing, u.beloproving, u.belocitying, Calling_Number as mdn, count(*) as calling_cnt, TOTAL_cnt
               |from ${baseTable} u
               |inner join ${baseMsgTable} b on (u.custid_calling=b.custid and u.beloproving=b.beloprov and u.belocitying=b.belocity)
               |group by custid_calling, u.custnameing, u.beloproving, u.belocitying, Calling_Number, TOTAL_cnt
             """.stripMargin).registerTempTable(abnormalCalling_table)

    //上面两个合并on cust+mdn   平均短信数判断异常卡？
    val abnormalDF = sqlContext.sql(
      s"""
         |select a.custid_call, a.custnameed, a.beloproved, a.belocityed, a.mdn, b.calling_cnt, a.called_cnt, (b.calling_cnt + a.called_cnt) as calls_cnt, a.TOTAL_cnt
         |from $abnormalCalled_table a
         |inner join $abnormalCalling_table b
         |on(a.custid_call=b.custid_call and a.mdn=b.mdn)
       """.stripMargin).filter("calls_cnt*100/TOTAL_cnt<50 or calls_cnt*100/TOTAL_cnt>150")

//    val abnormalMsg = abnormalDF.selectExpr("custid_call as custid", s"'${d}' as summ_cycle", "mdn",
//      "calling_cnt", "called_cnt", "calls_cnt",
//      "'-1' as upPacket", "'-1' as downPacket", "'-1' as totalPacket")
    val abnormalMsg = abnormalDF.selectExpr("'DAY' as gather_cycle", s"'${d}' as gather_date",
      "'ABNORMAL_MSGS' as gather_type", "'-1' as dim_obj", "beloproved as regprovince", "belocityed as regcity",
      "'MSGS' as net_type", "custid_call", "custnameed",  "mdn",
      "'-1' as udata", "'-1' as ddata", "'-1' as tdata",
      "calling_cnt as sendmsg", "called_cnt as recemsg", "calls_cnt as tmsg", "'-1' as avgtdata", "TOTAL_cnt as avgtmsg")
    //异常卡短信保存到hdfs
    abnormalMsg.repartition(10).write.format("orc").mode(SaveMode.Overwrite).save(outputPath + "/" + d + "/abnormalMsg")

    insertMSGSAbnormalByJDBC(outputPath + "/" + d + "/abnormalMsg", tableName, tableName+"_msgs")

    def insertMSGSAbnormalByJDBC(outputPath : String, tablename : String, bpname : String) = {
      // 将结果写入到tidb, 需要调整为upsert
      var dbConn = DbUtils.getDBConnection
      dbConn.setAutoCommit(false)
      val sql =
        s"""
           |insert into $tablename
           |(gather_cycle, gather_date, gather_type, dim_obj, regprovince, regcity, net_type, custid, custname, mdn,
           |udata, ddata, tdata, sendmsg, recemsg, tmsg, avgtdata, avgtmsg)
           |values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
       """.stripMargin

      val pstmt = dbConn.prepareStatement(sql)
      val result = sqlContext.read.format("orc").load(outputPath)
        .filter("length(regcity)>0")
        .map(x=>(x.getString(0), x.getString(1),
          x.getString(2),x.getString(3), x.getString(4), x.getString(5),
          x.getString(6),x.getString(7),x.getString(8),x.getString(9),
          x.getString(10),x.getString(11),x.getString(12),
          x.getLong(13),x.getLong(14),x.getLong(15),x.getString(16),x.getLong(17))).collect()

      var i = 0
      for(r<-result){
        val gather_cycle = r._1
        val gather_date = r._2
        val gather_type = r._3
        val dim_obj = r._4
        val regprovince = r._5
        val regcity = r._6
        val net_type = r._7
        val custid = r._8
        val custname = r._9
        val mdn = r._10
        val udata = r._11
        val ddata = r._12
        val tdata = r._13
        val sendmsg = r._14
        val recemsg = r._15
        val tmsg = r._16
        val avgtdata = r._17
        val avgtmsg =  r._18

        pstmt.setString(1, gather_cycle)
        pstmt.setString(2, gather_date)
        pstmt.setString(3, gather_type)
        pstmt.setString(4, dim_obj)
        pstmt.setString(5, regprovince)
        pstmt.setString(6, regcity)
        pstmt.setString(7, net_type)
        pstmt.setString(8, custid)
        pstmt.setString(9, custname)
        pstmt.setString(10, mdn)
        pstmt.setLong(11, udata.toLong)
        pstmt.setLong(12, ddata.toLong)
        pstmt.setLong(13, tdata.toLong)
        pstmt.setLong(14, sendmsg.toLong)
        pstmt.setLong(15, recemsg.toLong)
        pstmt.setLong(16, tmsg.toLong)
        pstmt.setLong(17, avgtdata.toLong)
        pstmt.setLong(18, avgtmsg.toLong)

        i += 1
        pstmt.addBatch()
        if (i % 1000 == 0) {
          pstmt.executeBatch
          dbConn.commit()
        }
      }
      pstmt.executeBatch
      dbConn.commit()
      pstmt.close()
      dbConn.close()

      CommonUtils.updateBreakTable(bpname, d)
    }


  }
}
