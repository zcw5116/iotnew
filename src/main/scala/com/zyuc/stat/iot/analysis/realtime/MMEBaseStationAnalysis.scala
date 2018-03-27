package com.zyuc.stat.iot.analysis.realtime

import com.zyuc.stat.iot.analysis.util.{HbaseDataUtil, MMEBaseStationHtableConverter}
import com.zyuc.stat.properties.ConfigProperties
import com.zyuc.stat.utils.{DateUtils, HbaseUtils}
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{Logging, SparkConf, SparkContext}

/**
  * 认证日志实时分析
  *
  * @author zhoucw
  * @version 1.0
  *
  */
object MMEBaseStationAnalysis extends Logging {


  /**
    * 主函数
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    sqlContext.sql("use " + ConfigProperties.IOT_HIVE_DATABASE)

    // 获取参数
    val appName = sc.getConf.get("spark.app.name", "name_201710231640") // name_201708010040

    val resultBSHtable = sc.getConf.get("spark.app.htable.resultBSHtable", "analyze_summ_rst_bs_mme")
    val areaRankHtable = sc.getConf.get("spark.app.htable.areaRankHtable", "analyze_summ_rst_area_rank")
    val bs4gTable = sc.getConf.get("spark.app.table.bs4gTable", "iot_basestation_4g")

    val analyzeBPHtable = sc.getConf.get("spark.app.htable.analyzeBPHtable", "analyze_bp_tab")


    // 实时分析类型： 0-后续会离线重跑数据, 2-后续不会离线重跑数据
    val progRunType = sc.getConf.get("spark.app.progRunType", "0")
    // 距离当前历史同期的天数
    val hisDayNumStr = sc.getConf.get("spark.app.hisDayNums", "7")

    if (progRunType != "0" && progRunType != "1") {
      logError("param progRunType invalid, expect:0|1")
      return
    }

    // resultDayHtable
    val families = new Array[String](1)
    families(0) = "r"
    HbaseUtils.createIfNotExists(areaRankHtable, families)


    def areaAnalysis(dataTime: String, analyType: String): Unit = {
      val dataDayid = dataTime.substring(0, 8)
      val oneHoursAgo = DateUtils.timeCalcWithFormatConvertSafe(dataTime, "yyyyMMddHHmm", -60 * 60, "yyyyMMddHHmm")
      if (analyType != "h" && analyType != "d") {
        logError("analyType 参数必须是h或者d")
        return
      }
      val rowStart = if (analyType == "h") oneHoursAgo + "_P000000000" else dataDayid
      val rowEnd = dataTime + "_P999999999"

      val hbaseDF = MMEBaseStationHtableConverter.convertToDF(sc, sqlContext, resultBSHtable, (rowStart, rowEnd))
      val tmpHtable = "tmpHtable_" + dataTime
      hbaseDF.registerTempTable(tmpHtable)

      val bsSQL =
        s"""
           |select h.cmpcodeAndServAndDomain,  b.provid, b.provname, b.cityid, b.cityname, b.enbid, b.zhlabel,
           |       h.ma_sn as rsn, h.ma_rn as rn
           |from ${tmpHtable} h, ${bs4gTable} b
           |where h.enbid = b.enbid and h.ma_rn > '0'
       """.stripMargin
      val bsTable = "bsTable_" + dataTime
      sqlContext.sql(bsSQL).cache().registerTempTable(bsTable)

      val bsStatSQL =
        s"""
           |select cmpcodeAndServAndDomain, enbid, zhlabel,
           |       sum(rsn) as rsn, sum(rn) as rn, sum(rsn)/sum(rn) as rat,
           |       row_number() over(partition by cmpcodeAndServAndDomain order by sum(rsn)/sum(rn) desc, sum(rn) desc) rank
           |from ${bsTable}
           |group by cmpcodeAndServAndDomain, enbid, zhlabel
       """.stripMargin

      val bsStatRDD = sqlContext.sql(bsStatSQL).filter("rank<1000").coalesce(5).rdd.map(x => {
        val rank = String.format("%3s", x(6).toString).replaceAll(" ", "0")
        val rowkey = x(0).toString + "_" + rank
        val bid = x(1).toString
        val bname = x(2).toString
        val rsn = x(3).toString
        val rn = x(4).toString
        val rat = x(5).toString
        val put = new Put(Bytes.toBytes(rowkey))

        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("ma_" + analyType + "_t_rn_bd"), Bytes.toBytes(bid))
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("ma_" + analyType + "_t_rn_bn"), Bytes.toBytes(bname))
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("ma_" + analyType + "_t_rn_bv"), Bytes.toBytes(rn))

        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("ma_" + analyType + "_t_rat_bd"), Bytes.toBytes(bid))
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("ma_" + analyType + "_t_rat_bn"), Bytes.toBytes(bname))
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("ma_" + analyType + "_t_rat_bv"), Bytes.toBytes(rat))
        (new ImmutableBytesWritable, put)

      })

      HbaseDataUtil.saveRddToHbase(areaRankHtable, bsStatRDD)

      val cityStatSQL =
        s"""
           |select cmpcodeAndServAndDomain, cityid, cityname,
           |       sum(rsn) as rsn, sum(rn) as rn, sum(rsn)/sum(rn) as rat,
           |       row_number() over(partition by cmpcodeAndServAndDomain order by sum(rsn)/sum(rn) desc, sum(rn) desc) rank
           |from ${bsTable}
           |group by cmpcodeAndServAndDomain, cityid, cityname
       """.stripMargin

      val cityStatRDD = sqlContext.sql(cityStatSQL).filter("rank<1000").coalesce(5).rdd.map(x => {
        val rank = String.format("%3s", x(6).toString).replaceAll(" ", "0")
        val rowkey = x(0).toString + "_" + rank
        val cid = x(1).toString
        val cname = x(2).toString
        val rsn = x(3).toString
        val rn = x(4).toString
        val rat = x(5).toString
        val put = new Put(Bytes.toBytes(rowkey))

        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("ma_" + analyType + "_t_rn_rd"), Bytes.toBytes(cid))
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("ma_" + analyType + "_t_rn_rn"), Bytes.toBytes(cname))
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("ma_" + analyType + "_t_rn_rv"), Bytes.toBytes(rn))

        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("ma_" + analyType + "_t_rat_rd"), Bytes.toBytes(cid))
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("ma_" + analyType + "_t_rat_rn"), Bytes.toBytes(cname))
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("ma_" + analyType + "_t_rat_rv"), Bytes.toBytes(rat))
        (new ImmutableBytesWritable, put)

      })

      HbaseDataUtil.saveRddToHbase(areaRankHtable, cityStatRDD)


      val provStatSQL =
        s"""
           |select cmpcodeAndServAndDomain, provid, provname,
           |       sum(rsn) as rsn, sum(rn) as rn, sum(rsn)/sum(rn) as rat,
           |       row_number() over(partition by cmpcodeAndServAndDomain order by sum(rsn)/sum(rn) desc, sum(rn) desc) rank
           |from ${bsTable}
           |group by cmpcodeAndServAndDomain, provid, provname
       """.stripMargin

      val provStatRDD = sqlContext.sql(provStatSQL).filter("rank<1000").coalesce(5).rdd.map(x => {
        val rank = String.format("%3s", x(6).toString).replaceAll(" ", "0")
        val rowkey = x(0).toString + "_" + rank
        val pid = x(1).toString
        val pname = x(2).toString
        val rsn = x(3).toString
        val rn = x(4).toString
        val rat = x(5).toString
        val put = new Put(Bytes.toBytes(rowkey))

        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("ma_" + analyType + "_t_rn_pd"), Bytes.toBytes(pid))
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("ma_" + analyType + "_t_rn_pn"), Bytes.toBytes(pname))
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("ma_" + analyType + "_t_rn_pv"), Bytes.toBytes(rn))

        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("ma_" + analyType + "_t_rat_pd"), Bytes.toBytes(pid))
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("ma_" + analyType + "_t_rat_pn"), Bytes.toBytes(pname))
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("ma_" + analyType + "_t_rat_pv"), Bytes.toBytes(rat))
        (new ImmutableBytesWritable, put)

      })

      HbaseDataUtil.saveRddToHbase(areaRankHtable, provStatRDD)

      // total
      val totalStatSQL =
        s"""
           |select cmpcodeAndServAndDomain,
           |       sum(rsn) as rsn, sum(rn) as rn, sum(rsn)/sum(rn) as rat
           |from ${bsTable}
           |group by cmpcodeAndServAndDomain
       """.stripMargin

      val totalStatRDD = sqlContext.sql(totalStatSQL).coalesce(5).rdd.map(x => {
        val rank = "000"
        val rowkey = x(0).toString + "_" + rank
        val rsn = x(1).toString
        val rn = x(2).toString
        val rat = x(3).toString

        val put = new Put(Bytes.toBytes(rowkey))
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("ma_" + analyType + "_t_rn_sv"), Bytes.toBytes(rn))
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("ma_" + analyType + "_t_rat_sv"), Bytes.toBytes(rat))

        (new ImmutableBytesWritable, put)
      })

      HbaseDataUtil.saveRddToHbase(areaRankHtable, totalStatRDD)

    }


    //  dataTime-当前数据时间  nextDataTime-下一个时刻数据的时间
    val dataTime = appName.substring(appName.lastIndexOf("_") + 1)
    areaAnalysis(dataTime, "d")
    areaAnalysis(dataTime, "h")



    // 更新时间, 断点时间比数据时间多1分钟
    val updateTime = DateUtils.timeCalcWithFormatConvertSafe(dataTime, "yyyyMMddHHmm", 1 * 60, "yyyyMMddHHmm")
    val analyzeColumn = if (progRunType == "0") "analyze_guess_bptime" else "analyze_real_bptime"
    HbaseUtils.upSertColumnByRowkey(analyzeBPHtable, "bp", "mme", analyzeColumn, updateTime)


  }

}
