package com.zyuc.stat.nbiot.analysis.tiDBmanager

import java.sql.PreparedStatement

import com.zyuc.iot.utils.DbUtils
import com.zyuc.stat.nbiot.etl.CRMETL2.struct
import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by liuzk on 18-7-26.
  */
object Upsert_USER_BASIC_FromDB {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()//.setAppName("test_20180723").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    val appName = sc.getConf.get("spark.app.name")
    val lastday = sc.getConf.get("spark.app.lastday","20180730")
    val inputPath = sc.getConf.get("spark.app.inputpath", "/user/iot/data/metadata/CRM")
    val outputPath = sc.getConf.get("spark.app.outputpath", "/user/iot/data/CRM/data/")

    val dataTime = appName.substring(appName.lastIndexOf("_") + 1)//20180723
    val yestarday = lastday.toInt
    val yestardayTable= "yestardayTable"
    sqlContext.read.format("orc").load(outputPath + yestarday).registerTempTable(yestardayTable)

    val inputfiles = inputPath + "/JiTuanWangYun-DuanDaoDuanBaoZhangXiTong_" + dataTime + ".txt"
    val rdd = sc.textFile(inputfiles).map(x=>x.split("\t",32))//.filter(_.length==28)
      .map(x=>Row(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8),x(9),
      x(10),x(11),x(12),x(13),x(14),x(15),x(16),x(17),x(18),x(19),
      x(20),x(21),x(22),x(23),x(24),x(25),x(26),x(27),x(28),x(29),x(30),x(31)))
    val df = sqlContext.createDataFrame(rdd,struct)
    df.repartition(20)
      .selectExpr("*","md5(concat(ProductID, CUST_ID, CUST_BELO_ENTE, BELO_CITY_COMP, BELO_PROV_COMP, IND_TYPE, IND_DET_TYPE, PROD_TYPE, ActiveTime, CmpTime, STAT, IMSI_3G, IMSI_4G, ICCID, IF_VPDN, IF_DRTSERV, IF_2G, IF_3G, IF_4G, IF_FLUX, IF_RegLimit, IF_VPDN_CN2, IF_MCB, RegLimitProv, IF_Rcv_SM, IF_Send_SM, IF_P2P_SM, DRTIP, Domain, APN, IF_VPDNAAA)) as md5")
      .write.format("orc").mode(SaveMode.Overwrite).save(outputPath + dataTime)

    val todayTable = "todayTable"
    sqlContext.read.format("orc").load(outputPath + dataTime).registerTempTable(todayTable)
    val fulltable = "fulltable"
    sqlContext.sql(
      s"""
         |select t.* ,y.MDN as yestmdn
         |from
         |${todayTable} t
         |full join
         |${yestardayTable} y
         |on t.MDN = y.MDN and t.md5=y.md5
       """.stripMargin).coalesce(20).write.format("orc").mode(SaveMode.Overwrite).save(outputPath + fulltable)

    // upsert new/today
    val insertResult = sqlContext.read.format("orc").load(outputPath + fulltable).filter("yestmdn is null").collect()
    val insertSql =
      s"""
         |insert into IOT_USER_BASIC_STATIC
         |(MDN,ProductID,CUST_ID,CUST_BELO_ENTE,BELO_CITY_COMP,BELO_PROV_COMP,IND_TYPE,IND_DET_TYPE,PROD_TYPE,ActiveTime,CmpTime,STAT,IMSI_3G,IMSI_4G,ICCID,IF_VPDN,IF_DRTSERV,IF_2G,IF_3G,IF_4G,IF_FLUX,IF_RegLimit,IF_VPDN_CN2,IF_MCB,RegLimitProv,IF_Rcv_SM,IF_Send_SM,IF_P2P_SM,DRTIP,Domain,APN,IF_VPDNAAA,md5,datatime)
         |values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
         |on duplicate key update ProductID=?,CUST_ID=?,CUST_BELO_ENTE=?,BELO_CITY_COMP=?,BELO_PROV_COMP=?,IND_TYPE=?,IND_DET_TYPE=?,PROD_TYPE=?,ActiveTime=?,CmpTime=?,STAT=?,IMSI_3G=?,IMSI_4G=?,ICCID=?,IF_VPDN=?,IF_DRTSERV=?,IF_2G=?,IF_3G=?,IF_4G=?,IF_FLUX=?,IF_RegLimit=?,IF_VPDN_CN2=?,IF_MCB=?,RegLimitProv=?,IF_Rcv_SM=?,IF_Send_SM=?,IF_P2P_SM=?,DRTIP=?,Domain=?,APN=?,IF_VPDNAAA=?,md5=?,datatime=?
       """.stripMargin

    var dbConn = DbUtils.getDBConnection
    dbConn.setAutoCommit(false)
    val pstmt = dbConn.prepareStatement(insertSql)
    var num = 0
    for(r<-insertResult){
      val a1 = r.getString(0)
      val a2 = r.getString(1)
      val a3 = r.getString(2)
      val a4 = r.getString(3)
      val a5 = r.getString(4)
      val a6 = r.getString(5)
      val a7 = r.getString(6)
      val a8 = r.getString(7)
      val a9 = r.getString(8)
      val a10 = r.getString(9)
      val a11 = r.getString(10)
      val a12 = r.getString(11)
      val a13 = r.getString(12)
      val a14 = r.getString(13)
      val a15 = r.getString(14)
      val a16 = r.getString(15)
      val a17 = r.getString(16)
      val a18 = r.getString(17)
      val a19 = r.getString(18)
      val a20 = r.getString(19)
      val a21 = r.getString(20)
      val a22 = r.getString(21)
      val a23 = r.getString(22)
      val a24 = r.getString(23)
      val a25 = r.getString(24)
      val a26 = r.getString(25)
      val a27 = r.getString(26)
      val a28 = r.getString(27)
      val a29 = r.getString(28)
      val a30 = r.getString(29)
      val a31 = r.getString(30)
      val a32 = r.getString(31)
      val md5 = r.getString(32)

      pstmt.setString(1, "86"+a1)
      pstmt.setString(2, a2)
      pstmt.setLong(3, a3.toLong)
      pstmt.setString(4, a4)
      pstmt.setString(5, a5)
      pstmt.setString(6, a6)
      pstmt.setString(7, a7)
      pstmt.setString(8, a8)
      pstmt.setString(9, a9)
      pstmt.setString(10, a10)
      pstmt.setString(11, a11)
      pstmt.setString(12, a12)
      pstmt.setString(13, a13)
      pstmt.setString(14, a14)
      pstmt.setString(15, a15)
      pstmt.setString(16, a16)
      pstmt.setString(17, a17)
      pstmt.setString(18, a18)
      pstmt.setString(19, a19)
      pstmt.setString(20, a20)
      pstmt.setString(21, a21)
      pstmt.setString(22, a22)
      pstmt.setString(23, a23)
      pstmt.setString(24, a24)
      pstmt.setString(25, a25)
      pstmt.setString(26, a26)
      pstmt.setString(27, a27)
      pstmt.setString(28, a28)
      pstmt.setString(29, a29)
      pstmt.setString(30, a30)
      pstmt.setString(31, a31)
      pstmt.setString(32, a32)
      pstmt.setString(33, md5)
      pstmt.setString(34, dataTime)

      pstmt.setString(35, a2)
      pstmt.setLong(36, a3.toLong)
      pstmt.setString(37, a4)
      pstmt.setString(38, a5)
      pstmt.setString(39, a6)
      pstmt.setString(40, a7)
      pstmt.setString(41, a8)
      pstmt.setString(42, a9)
      pstmt.setString(43, a10)
      pstmt.setString(44, a11)
      pstmt.setString(45, a12)
      pstmt.setString(46, a13)
      pstmt.setString(47, a14)
      pstmt.setString(48, a15)
      pstmt.setString(49, a16)
      pstmt.setString(50, a17)
      pstmt.setString(51, a18)
      pstmt.setString(52, a19)
      pstmt.setString(53, a20)
      pstmt.setString(54, a21)
      pstmt.setString(55, a22)
      pstmt.setString(56, a23)
      pstmt.setString(57, a24)
      pstmt.setString(58, a25)
      pstmt.setString(59, a26)
      pstmt.setString(60, a27)
      pstmt.setString(61, a28)
      pstmt.setString(62, a29)
      pstmt.setString(63, a30)
      pstmt.setString(64, a31)
      pstmt.setString(65, a32)
      pstmt.setString(66, md5)
      pstmt.setString(67, dataTime)
      num += 1
      pstmt.addBatch()
      if (num % 1000 == 0) {
        pstmt.executeBatch
        dbConn.commit()
      }
    }
    pstmt.executeBatch
    dbConn.commit()
    pstmt.close()
    dbConn.close()

    //detele old/yesterday
/*    val deleteResult = sqlContext.read.format("orc").load(outputPath + fulltable).filter("MDN is null").collect()
    var delString = ""
    var delNums = 0
    for(delS<-deleteResult){
      delString = delString + delS.getString(29) + ","
      delNums =delNums +1
    }
    if(delString.lastIndexOf(",") != -1){
      delString = delString.substring(0,delString.lastIndexOf(","))
    }

    //val delNums = deleteResult.count()
    val deletetimes = (delNums/5000).toInt + 1
    val deleteSql = s"delete from IOT_USER_BASIC_STATIC where a1 in (${delString}) limit 5000"
    var pstmtDel: PreparedStatement = null
    var dbConn1 = DbUtils.getDBConnection

    pstmtDel = dbConn1.prepareStatement(deleteSql)
    for(i<-1 to deletetimes){
      pstmtDel.executeUpdate()
    }
    pstmtDel.close()
    dbConn1.close()*/

  }

}
