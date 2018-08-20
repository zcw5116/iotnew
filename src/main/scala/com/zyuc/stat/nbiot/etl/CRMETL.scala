package com.zyuc.stat.nbiot.etl

import com.zyuc.iot.utils.DbUtils
import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
  * Created by liuzk on 18-7-25.
  */
object CRMETL {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()//.setAppName("test_20180723").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    val appName = sc.getConf.get("spark.app.name")
    val inputPath = sc.getConf.get("spark.app.inputpath", "/user/iot/data/metadata/CRM")
    val outputPath = sc.getConf.get("spark.app.outputpath", "/user/iot/data/CRM/data/")

    val dataTime = appName.substring(appName.lastIndexOf("_") + 1)//20180723
    val inputfiles = inputPath + "/JiTuanWangYun-DuanDaoDuanBaoZhangXiTong_" + dataTime + ".txt"
    val rdd = sc.textFile(inputfiles).map(x=>x.split("\t")).filter(_.length==28)
      .map(x=>Row(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8),x(9),
        x(10),x(11),x(12),x(13),x(14),x(15),x(16),x(17),x(18),x(19),
        x(20),x(21),x(22),x(23),x(24),x(25),x(26),x(27)))
    val df = sqlContext.createDataFrame(rdd,struct)
    df.repartition(20)
      .selectExpr("*","md5(concat(a2, a3, a4, a5, a6, a7, a8, a9, a10, a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r)) as md5")
      .write.format("orc").mode(SaveMode.Overwrite).save(outputPath + dataTime)

    var dbConn = DbUtils.getDBConnection
    dbConn.setAutoCommit(false)
    val sql =
      s"""
         |insert into IOT_USER_BASIC_STATIC
         |(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, md5)
         |values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
         |on duplicate key update a2=?
       """.stripMargin

    val pstmt = dbConn.prepareStatement(sql)
    val result = sqlContext.read.format("orc").load(outputPath + dataTime)
      .map(x=>((x.getString(0), x.getString(1), x.getString(2), x.getString(3),
        x.getString(4), x.getString(5), x.getString(6), x.getString(7),
        x.getString(8), x.getString(9), x.getString(10), x.getString(11),
        x.getString(12), x.getString(13)), (x.getString(14), x.getString(15),
        x.getString(16), x.getString(17), x.getString(18), x.getString(19),
        x.getString(20), x.getString(21), x.getString(22), x.getString(23),
        x.getString(24), x.getString(25), x.getString(26), x.getString(27), x.getString(28)))).collect()

    var num = 0
    for(r<-result){
      val a1 = r._1._1
      val a2 = r._1._2
      val a3 = r._1._3
      val a4 = r._1._4
      val a5 = r._1._5
      val a6 = r._1._6
      val a7 = r._1._7
      val a8 = r._1._8
      val a9 = r._1._9
      val a10 = r._1._10
      val a = r._1._11
      val b = r._1._12
      val c = r._1._13
      val d = r._1._14
      val e = r._2._1
      val f = r._2._2
      val g = r._2._3
      val h = r._2._4
      val i = r._2._5
      val j = r._2._6
      val k = r._2._7
      val l = r._2._8
      val m = r._2._9
      val n = r._2._10
      val o = r._2._11
      val p = r._2._12
      val q = r._2._13
      val rrrr = r._2._14
      val md5 = r._2._15

      pstmt.setString(1, a1)
      pstmt.setString(2, a2)
      pstmt.setString(3, a3)
      pstmt.setString(4, a4)
      pstmt.setString(5, a5)
      pstmt.setString(6, a6)
      pstmt.setString(7, a7)
      pstmt.setString(8, a8)
      pstmt.setString(9, a9)
      pstmt.setString(10, a10)
      pstmt.setString(11, a)
      pstmt.setString(12, b)
      pstmt.setString(13, c)
      pstmt.setString(14, d)
      pstmt.setString(15, e)
      pstmt.setString(16, f)
      pstmt.setString(17, g)
      pstmt.setString(18, h)
      pstmt.setString(19, i)
      pstmt.setString(20, j)
      pstmt.setString(21, k)
      pstmt.setString(22, l)
      pstmt.setString(23, m)
      pstmt.setString(24, n)
      pstmt.setString(25, o)
      pstmt.setString(26, p)
      pstmt.setString(27, q)
      pstmt.setString(28, rrrr)

      pstmt.setString(29, md5)
      pstmt.setString(30, a2)

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


  }
  val struct = StructType(Array(
    StructField("a1", StringType),
    StructField("a2", StringType),
    StructField("a3", StringType),
    StructField("a4", StringType),
    StructField("a5", StringType),
    StructField("a6", StringType),
    StructField("a7", StringType),
    StructField("a8", StringType),
    StructField("a9", StringType),
    StructField("a10", StringType),
    StructField("a", StringType),
    StructField("b", StringType),
    StructField("c", StringType),
    StructField("d", StringType),
    StructField("e", StringType),
    StructField("f", StringType),
    StructField("g", StringType),
    StructField("h", StringType),
    StructField("i", StringType),
    StructField("j", StringType),
    StructField("k", StringType),
    StructField("l", StringType),
    StructField("m", StringType),
    StructField("n", StringType),
    StructField("o", StringType),
    StructField("p", StringType),
    StructField("q", StringType),
    StructField("r", StringType)
  ))

}
