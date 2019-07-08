package com.zyuc.stat.iot.etl.secondaryTotal.CdrBaseOnProvAnalysis

import com.zyuc.iot.utils.DbUtils
import com.zyuc.stat.nbiot.analysis.realtime.utils.CommonUtils
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by liuzk on 19-6-17.
  *
  * 基于原子基表 做网络维度的二次统计
  * 4G Ehrpd Evdo Cdma1x
  */
object CdrBaseOnProvAnalysisMonth {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()//.setMaster("local[2]").setAppName("name_20190617")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)

    val appName = sc.getConf.get("spark.app.name")
    val inputPathNB = sc.getConf.get("spark.app.inputPathNB", "/user/iot_ete/data/cdr/summ_d/nb/")
    val inputPathPgw = sc.getConf.get("spark.app.inputPathPgw", "/user/iot_ete/data/cdr/summ_d/pgw/")
    val inputPathPdsn = sc.getConf.get("spark.app.inputPathPdsn", "/user/iot_ete/data/cdr/summ_d/pdsn/")
    val outputPath = sc.getConf.get("spark.app.outputPath","/user/iot_ete/data/cdr/summ_d/baseOnProv/month/")
    val userPath = sc.getConf.get("spark.app.userPath", "/user/iot/data/baseuser/data/")
    val userDataTime = sc.getConf.get("spark.app.userDataTime", "20180510")

    val dataTime = appName.substring(appName.lastIndexOf("_") + 1)
    val dayid = dataTime.substring(0, 8)
    val monthid = dataTime.substring(0, 6)


    try{
      //---------NB
      val tempTableNB = "tempTableNB"
      sqlContext.read.format("orc").load(inputPathNB + "dayid=" + monthid + "*")
        .registerTempTable(tempTableNB)

      val resultDF_nb = sqlContext.sql(
        s"""
           |select own_provid, own_lanid, industry_level1, industry_level2, provid, lanid,
           |       sum(upflow) upflow, sum(downflow) downflow, sum(upflow + downflow) totalflow,
           |       count(distinct mdn) activeusers, sum(duration) duration, sum(times) times, 'NB' nettype
           |from $tempTableNB
           |group by own_provid, own_lanid, industry_level1, industry_level2, provid, lanid
       """.stripMargin)

      val df = resultDF_nb.selectExpr("own_provid","regexp_replace(own_lanid,'电信','') as own_lanid",
        "industry_level1","industry_level2","provid","lanid",
        "upflow","downflow","totalflow","activeusers","duration","times","nettype")

      df.select(df("own_provid").cast("string"),
        df("own_lanid").cast("string"),
        df("industry_level1").cast("string"),
        df("industry_level2").cast("string"),
        df("provid").cast("string"),
        df("lanid").cast("string"),
        df("upflow").cast("double"),
        df("downflow").cast("double"),
        df("totalflow").cast("double"),
        df("activeusers").cast("long"),
        df("duration").cast("double"),
        df("times").cast("long"),
        df("nettype").cast("string"))
        .coalesce(10).write.mode(SaveMode.Overwrite).format("orc").save(outputPath + "NB/" + dayid)

      insert2Tidb("iot_ana_prov_access_stat_nb_day",outputPath + "NB/" + dayid)

    }catch{
      case e: Exception => {
        e.printStackTrace()
        print("------------NB话单当前无数据-----------")
      }
    }

    try{
      //---------4G : 4G话单中取rat='6'
      val tempTable4G = "tempTable4G"
      sqlContext.read.format("orc").load(inputPathPgw + "dayid=" + monthid + "*").filter("rattype='6'")
        .registerTempTable(tempTable4G)

      val resultDF_4g = sqlContext.sql(
        s"""
           |select own_provid, own_lanid, industry_level1, industry_level2, provid, lanid,
           |       sum(upflow) upflow, sum(downflow) downflow, sum(upflow + downflow) totalflow,
           |       count(distinct mdn) activeusers, sum(duration) duration, sum(times) times, '4G' nettype
           |from $tempTable4G
           |group by own_provid, own_lanid, industry_level1, industry_level2, provid, lanid
       """.stripMargin)

      val df = resultDF_4g.selectExpr("own_provid","regexp_replace(own_lanid,'电信','') as own_lanid",
        "industry_level1","industry_level2","provid","lanid",
        "upflow","downflow","totalflow","activeusers","duration","times","nettype")

      df.select(df("own_provid").cast("string"),
        df("own_lanid").cast("string"),
        df("industry_level1").cast("string"),
        df("industry_level2").cast("string"),
        df("provid").cast("string"),
        df("lanid").cast("string"),
        df("upflow").cast("double"),
        df("downflow").cast("double"),
        df("totalflow").cast("double"),
        df("activeusers").cast("long"),
        df("duration").cast("double"),
        df("times").cast("long"),
        df("nettype").cast("string"))
        .coalesce(10).write.mode(SaveMode.Overwrite).format("orc").save(outputPath + "4G/" + dayid)

      insert2Tidb("iot_ana_prov_access_stat_4g_day",outputPath + "4G/" + dayid)
        //.selectExpr("regexp_replace(own_lanid, '电信', '') as own_lanid",

    }catch{
      case e: Exception => {
        e.printStackTrace()
        print("------------4G话单当前无数据-----------")
      }
    }


    try{
      //---------Ehrpd : 4G话单中取rat='102'
      val tempTableEhrpd = "tempTableEhrpd"
      sqlContext.read.format("orc").load(inputPathPgw + "dayid=" + monthid + "*").filter("rattype='102'")
        .registerTempTable(tempTableEhrpd)

      val resultDF_Ehrpd = sqlContext.sql(
        s"""
           |select own_provid, own_lanid, industry_level1, industry_level2, provid, '未知' as lanid,
           |       sum(upflow) upflow, sum(downflow) downflow, sum(upflow + downflow) totalflow,
           |       count(distinct mdn) activeusers, sum(duration) duration, sum(times) times, 'EHRPD' nettype
           |from $tempTableEhrpd
           |group by own_provid, own_lanid, industry_level1, industry_level2, provid
       """.stripMargin)

      val df = resultDF_Ehrpd.selectExpr("own_provid","regexp_replace(own_lanid,'电信','') as own_lanid",
        "industry_level1","industry_level2","provid","lanid",
        "upflow","downflow","totalflow","activeusers","duration","times","nettype")

      df.select(df("own_provid").cast("string"),
        df("own_lanid").cast("string"),
        df("industry_level1").cast("string"),
        df("industry_level2").cast("string"),
        df("provid").cast("string"),
        df("lanid").cast("string"),
        df("upflow").cast("double"),
        df("downflow").cast("double"),
        df("totalflow").cast("double"),
        df("activeusers").cast("long"),
        df("duration").cast("double"),
        df("times").cast("long"),
        df("nettype").cast("string"))
        .coalesce(10).write.mode(SaveMode.Overwrite).format("orc").save(outputPath + "Ehrpd/" + dayid)

      insert2Tidb("iot_ana_prov_access_stat_ehrpd_day",outputPath + "Ehrpd/" + dayid)

    }catch{
      case e: Exception => {
        e.printStackTrace()
        print("------------4G话单Ehrpd当前无数据-----------")
      }
    }


    try{
      //---------Evdo 3g : pdsn话单中取service_option='59'
      val tempTableEvdo = "tempTableEvdo"
      sqlContext.read.format("orc").load(inputPathPdsn + "dayid=" + monthid + "*").filter("service_option='59'")
        .registerTempTable(tempTableEvdo)

      val resultDF_Evdo = sqlContext.sql(
        s"""
           |select own_provid, own_lanid, industry_level1, industry_level2, provid, lanid,
           |       sum(upflow) upflow, sum(downflow) downflow, sum(upflow + downflow) totalflow,
           |       count(distinct mdn) activeusers, sum(duration) duration, sum(times) times, 'EVDO' nettype
           |from $tempTableEvdo
           |group by own_provid, own_lanid, industry_level1, industry_level2, provid, lanid
       """.stripMargin)

      val df = resultDF_Evdo.selectExpr("own_provid","regexp_replace(own_lanid,'电信','') as own_lanid",
        "industry_level1","industry_level2","provid","lanid",
        "upflow","downflow","totalflow","activeusers","duration","times","nettype")

      df.select(df("own_provid").cast("string"),
        df("own_lanid").cast("string"),
        df("industry_level1").cast("string"),
        df("industry_level2").cast("string"),
        df("provid").cast("string"),
        df("lanid").cast("string"),
        df("upflow").cast("double"),
        df("downflow").cast("double"),
        df("totalflow").cast("double"),
        df("activeusers").cast("long"),
        df("duration").cast("double"),
        df("times").cast("long"),
        df("nettype").cast("string"))
        .coalesce(10).write.mode(SaveMode.Overwrite).format("orc").save(outputPath + "Evdo/" + dayid)

      insert2Tidb("iot_ana_prov_access_stat_evdo_day",outputPath + "Evdo/" + dayid)

      //---------Cdma1x 2g : pdsn话单中取service_option='33'
      val tempTableCdma1x = "tempTableCdma1x"
      sqlContext.read.format("orc").load(inputPathPdsn + "dayid=" + monthid + "*").filter("service_option='33'")
        .registerTempTable(tempTableCdma1x)

      val resultDF_Cdma1x = sqlContext.sql(
        s"""
           |select own_provid, own_lanid, industry_level1, industry_level2, provid, lanid,
           |       sum(upflow) upflow, sum(downflow) downflow, sum(upflow + downflow) totalflow,
           |       count(distinct mdn) activeusers, sum(duration) duration, sum(times) times, 'CDMA1X' nettype
           |from $tempTableCdma1x
           |group by own_provid, own_lanid, industry_level1, industry_level2, provid, lanid
       """.stripMargin)

      val df1 = resultDF_Cdma1x.selectExpr("own_provid","regexp_replace(own_lanid,'电信','') as own_lanid",
        "industry_level1","industry_level2","provid","lanid",
        "upflow","downflow","totalflow","activeusers","duration","times","nettype")

      df1.select(df1("own_provid").cast("string"),
        df1("own_lanid").cast("string"),
        df1("industry_level1").cast("string"),
        df1("industry_level2").cast("string"),
        df1("provid").cast("string"),
        df1("lanid").cast("string"),
        df1("upflow").cast("double"),
        df1("downflow").cast("double"),
        df1("totalflow").cast("double"),
        df1("activeusers").cast("long"),
        df1("duration").cast("double"),
        df1("times").cast("long"),
        df1("nettype").cast("string"))
        .coalesce(10).write.mode(SaveMode.Overwrite).format("orc").save(outputPath + "Cdma1x/" + dayid)

      insert2Tidb("iot_ana_prov_access_stat_cdma1x_month",outputPath + "Cdma1x/" + dayid)

    }catch{
      case e: Exception => {
        e.printStackTrace()
        print("------------3G话单Evdo,Cdma1x当前无数据-----------")
      }
    }


    // 将结果写入到tidb, 需要调整为upsert
    def insert2Tidb(tableBP:String, datapath:String) = {

      var dbConn = DbUtils.getDBConnection
      dbConn.setAutoCommit(false)
      val sql =
        s"""
           |insert into iot_ana_prov_access_stat
           |(summ_cycle, nettype, summ_day,
           |reg_prov, reg_city, ind_type, ind_det_type, acc_prov, acc_city,
           |flux_up, flux_down, flux_sum, act_user_num, act_use_duration, act_use_times)
           |values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
         """.stripMargin

      val pstmt = dbConn.prepareStatement(sql)
      val result = sqlContext.read.format("orc").load(datapath).
        map(x=>(x.getString(0), x.getString(1), x.getString(2),x.getString(3),x.getString(4),x.getString(5),
          x.getDouble(6),x.getDouble(7), x.getDouble(8), x.getLong(9), x.getDouble(10), x.getLong(11), x.getString(12))).collect()
      //x.getLong(8)  -> x.getDouble(8)
      var i = 0
      for(r<-result){
        val reg_prov = r._1
        val reg_city = r._2
        val ind_type = r._3
        val ind_det_type = r._4
        val acc_prov = r._5
        val acc_city = r._6
        val flux_up = r._7
        val flux_down = r._8
        val flux_sum = r._9
        val act_user_num = r._10
        val act_use_duration = r._11
        val act_use_times = r._12
        val nettype = r._13

        pstmt.setString(1, "MONTH")
        pstmt.setString(2, nettype)
        pstmt.setString(3, dayid)
        pstmt.setString(4, reg_prov)
        pstmt.setString(5, reg_city)
        pstmt.setString(6, ind_type)
        pstmt.setString(7, ind_det_type)
        pstmt.setString(8, acc_prov)
        pstmt.setString(9, acc_city)
        pstmt.setLong(10, flux_up.toLong)
        pstmt.setLong(11, flux_down.toLong)
        pstmt.setLong(12, flux_sum.toLong)
        pstmt.setInt(13, act_user_num.toInt)
        pstmt.setLong(14, act_use_duration.toLong)
        pstmt.setLong(15, act_use_times)

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
      ////////////////////
      CommonUtils.updateBreakTable(tableBP, monthid)
    }
    /*CREATE `iot_ana_prov_access_stat`(
      `summ_cycle` varchar(5),   --DAY WEEK MONTH
      `nettype` varchar(6),   -- 4G NB EHRPD EVDO CDMA1X
      `summ_day` varchar(8),  -- YYYYMMDD
      `reg_prov` varchar(50),
    `reg_city` varchar(50),
    `ind_type` varchar(50),
    `ind_det_type` varchar(200),
    `acc_prov` varchar(50),
    `acc_city` varchar(50),
    `flux_up` bigint(20),
    `flux_down` bigint(20),
    `flux_sum` bigint(20),
    `act_user_num` int(10),
    `act_use_duration` bigint(20),
    `act_use_times` bigint(20),
    KEY `iot_ana_prov_access_stat_index` (`summ_cycle`,`nettype`,`summ_day`,`reg_prov`,`reg_city`,`ind_type`,`ind_det_type`,`acc_prov`,`acc_city`)
    );*/
  }

}
