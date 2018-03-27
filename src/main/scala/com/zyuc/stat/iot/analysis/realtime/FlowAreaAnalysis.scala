package com.zyuc.stat.iot.analysis.realtime

import com.zyuc.stat.iot.analysis.util.AreaHtableConverter
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
object FlowAreaAnalysis extends Logging {

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
    val appName = sc.getConf.get("spark.app.name", "name_201710271340") // name_201708010040

    val resultBSHtable = sc.getConf.get("spark.app.htable.resultBSHtable", "analyze_summ_rst_bs")
    val areaRankHtable = sc.getConf.get("spark.app.htable.areaRankHtable", "analyze_summ_rst_area_rank")
    val bs4gTable = sc.getConf.get("spark.app.table.bs4gTable", "iot_basestation_4g")
    val bs3gTable = sc.getConf.get("spark.app.table.bs4gTable", "iot_basestation_3g")

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

    //  dataTime-当前数据时间  nextDataTime-下一个时刻数据的时间
    val dataTime = appName.substring(appName.lastIndexOf("_") + 1)
    areaAnalysis(dataTime, "d")
    areaAnalysis(dataTime, "h")

    def areaAnalysis(dataTime: String, analyType: String): Unit = {
      val dataDayid = dataTime.substring(0, 8)
      val oneHoursAgo = DateUtils.timeCalcWithFormatConvertSafe(dataTime, "yyyyMMddHHmm", -60 * 60, "yyyyMMddHHmm")
      if (analyType != "h" && analyType != "d") {
        logError("analyType para required: d or h")
        return
      }
      val rowStart = if (analyType == "h") oneHoursAgo + "_P000000000" else dataDayid
      val rowEnd = dataTime + "_P999999999"

      val hbaseDF = AreaHtableConverter.convertToDF(sc, sqlContext, resultBSHtable, (rowStart, rowEnd))
      val tmpHtable = "tmpHtable_" + dataTime
      hbaseDF.registerTempTable(tmpHtable)

      val bsSQL =
        s"""
           |select h.cAndSAndD, '3g' nettype,
           |       nvl(b.provcode, '0') provid, nvl(b.provname, '0') provname,
           |       nvl(b.citycode, '0') cityid, nvl(b.cityname, '0') cityname,
           |       nvl(h.enbid, '0') enbid,
           |       nvl(concat_ws('_', h.enbid, b.cityname), '0')  enbname,
           |       h.ma_sn, h.ma_rn,
           |       h.a_sn, h.a_rn,
           |       h.f_d, h.f_u
           |from ${tmpHtable} h left join ${bs3gTable} b on(substr(h.enbid, 1, 4) = b.bsidpre)
           |where h.nettype='3g'
           |union all
           |select h.cAndSAndD, '4g' nettype,
           |       nvl(b.provid, '0') provid, nvl(b.provname, '0') provname,
           |       nvl(b.cityid, '0') cityid, nvl(b.cityname, '0') cityname,
           |       nvl(b.enbid, '0') enbid,
           |       nvl(b.zhlabel,'0') enbname,
           |       h.ma_sn, h.ma_rn,
           |       h.a_sn, h.a_rn,
           |       h.f_d, h.f_u
           |from   ${tmpHtable} h left join ${bs4gTable} b on(h.enbid = b.enbid)
           |where  h.nettype='4g'
       """.stripMargin
      val bsTable = "bsTable_" + dataTime
      sqlContext.sql(bsSQL).repartition(30).registerTempTable(bsTable)


      val bsnetSql =
        s"""
           |    select cAndSAndD, provid, cityid, enbid, nettype,
           |           sum(ma_sn) ma_sn, sum(ma_rn) ma_rn,  nvl(sum(ma_sn)/sum(ma_rn)) ma_rat,
           |           sum(a_sn) a_sn, sum(a_rn) a_rn,  nvl(sum(a_sn)/sum(a_rn)) a_rat,
           |           sum(f_d) f_d, sum(f_u) f_u, (sum(f_d) + sum(f_u)) f_t
           |    from ${bsTable}
           |    group by cAndSAndD, provid, cityid, enbid, nettype
         """.stripMargin

      val bsNetTmpTable = "bsNetTmpTable_" + dataTime
      sqlContext.sql(bsnetSql).registerTempTable(bsNetTmpTable)

      val bsNetTable = "bsNetTable_" + dataTime
      sqlContext.sql(s"cache table ${bsNetTable} as select cAndSAndD, provid, cityid, enbid, nettype, ma_sn, ma_rn," +
        s"a_sn, a_rn, f_d, f_u, f_t from ${bsNetTmpTable} ")
      val re = sqlContext.sql(bsnetSql).repartition(30).cache().registerTempTable(bsNetTable)



      /*     val bsG3StatSQL =
        s"""
           |select cAndSAndD, nettype, provid, provname, cityid, cityname, enbid, zhlabel,
           |       ma_sn, ma_rn, ma_rat,
           |       a_sn, a_rn, a_rat,
           |       f_d, f_u, f_t,
           |       row_number() over(partition by cAndSAndD, provid, nettype order by ma_rn desc) as ma_n_p_rank,
           |       row_number() over(partition by cAndSAndD, cityid, nettype order by ma_rn desc) as ma_n_c_rank,
           |       row_number() over(partition by cAndSAndD, enbid, nettype order by ma_rn desc) as ma_n_b_rank,
           |       row_number() over(partition by cAndSAndD, provid order by ma_rn desc) as ma_t_p_rank,
           |       row_number() over(partition by cAndSAndD, cityid order by ma_rn desc) as ma_t_c_rank,
           |       row_number() over(partition by cAndSAndD, enbid order by ma_rn desc) as ma_t_b_rank,
           |       row_number() over(partition by cAndSAndD, provid, nettype order by a_rn desc) as a_p_rank,
           |       row_number() over(partition by cAndSAndD, cityid, nettype order by a_rn desc) as a_c_rank,
           |       row_number() over(partition by cAndSAndD, enbid, nettype order by a_rn desc) as a_b_rank,
           |       row_number() over(partition by cAndSAndD, provid, nettype order by f_t desc) as f_p_rank,
           |       row_number() over(partition by cAndSAndD, cityid, nettype order by f_t desc) as f_c_rank,
           |       row_number() over(partition by cAndSAndD, enbid, nettype order by f_t desc) as f_b_rank
           |from
           |(
           |select cAndSAndD, nettype, provid, provname, cityid, cityname, enbid, zhlabel,
           |       sum(ma_sn) ma_sn, sum(ma_rn) ma_rn,  nvl(sum(ma_sn)/sum(ma_rn)) ma_rat,
           |       sum(a_sn) a_sn, sum(a_rn) a_rn,  nvl(sum(a_sn)/sum(a_rn)) a_rat,
           |       sum(f_d) f_d, sum(f_u) f_u, (sum(f_d) + sum(f_u)) f_t
           | from ${bsG3Table}
           | group by cAndSAndD, nettype, provid, provname, cityid, cityname, enbid, zhlabel
           | grouping sets((cAndSAndD, provid, provname), (cAndSAndD, cityid, cityname), (cAndSAndD, enbid, zhlabel),
           | (cAndSAndD, provid, provname, nettype), (cAndSAndD, cityid, cityname, nettype), (cAndSAndD, enbid, zhlabel, nettype))
           | ) t
         """.stripMargin*/


/*
      val bsStatSQL =
        s"""
           |select cAndSAndD, nettype, provid, provname, cityid, cityname, enbid, zhlabel,
           |       sum(ma_sn) ma_sn, sum(ma_rn) ma_rn,  nvl(sum(ma_sn)/sum(ma_rn)) ma_rat,
           |       sum(a_sn) a_sn, sum(a_rn) a_rn,  nvl(sum(a_sn)/sum(a_rn)) a_rat,
           |       sum(f_d) f_d, sum(f_u) f_u, (sum(f_d) + sum(f_u)) f_t
           | from ${bsTable}
           | group by cAndSAndD, nettype, provid, provname, cityid, cityname, enbid, zhlabel
           | grouping sets((cAndSAndD, provid, provname), (cAndSAndD, cityid, cityname), (cAndSAndD, enbid, zhlabel),
           | (cAndSAndD, provid, provname, nettype), (cAndSAndD, cityid, cityname, nettype), (cAndSAndD, enbid, zhlabel, nettype))
         """.stripMargin

      val bsStatResult = sqlContext.sql(bsStatSQL)

      val bsResultTable = "bsResultTable_" + analyType + "_" + dataTime
      bsStatResult.registerTempTable(bsResultTable)
*/


      val provStatSQL =
        s"""
           |select cAndSAndD, provid,
           |       ma_sn, ma_rn, ma_rat,
           |       row_number() over(partition by cAndSAndD order by ma_rn desc ) ma_rank,
           |       a_sn, a_rn, a_rat,
           |       row_number() over(partition by cAndSAndD order by a_rn desc ) a_rank,
           |       f_d, f_u, f_t,
           |       row_number() over(partition by cAndSAndD order by f_t desc ) f_rank
           |from
           |(
           |    select cAndSAndD, provid,
           |           sum(ma_sn) ma_sn, sum(ma_rn) ma_rn,  nvl(sum(ma_sn)/sum(ma_rn)) ma_rat,
           |           sum(a_sn) a_sn, sum(a_rn) a_rn,  nvl(sum(a_sn)/sum(a_rn)) a_rat,
           |           sum(f_d) f_d, sum(f_u) f_u, (sum(f_d) + sum(f_u)) f_t
           |    from ${bsNetTable}
           |    group by cAndSAndD, provid
           | ) t
         """.stripMargin
      val provResult = sqlContext.sql(provStatSQL)

      val provNetStatSQL =
        s"""
           |select cAndSAndD, provid, nettype,
           |       ma_sn, ma_rn, ma_rat,
           |       row_number() over(partition by cAndSAndD, nettype order by ma_rn desc ) ma_rank,
           |       a_sn, a_rn, a_rat,
           |       row_number() over(partition by cAndSAndD, nettype order by a_rn desc ) a_rank,
           |       f_d, f_u, f_t,
           |       row_number() over(partition by cAndSAndD, nettype order by f_t desc ) f_rank
           |from
           |(
           |    select cAndSAndD, provid, nettype,
           |           sum(ma_sn) ma_sn, sum(ma_rn) ma_rn,  nvl(sum(ma_sn)/sum(ma_rn)) ma_rat,
           |           sum(a_sn) a_sn, sum(a_rn) a_rn,  nvl(sum(a_sn)/sum(a_rn)) a_rat,
           |           sum(f_d) f_d, sum(f_u) f_u, (sum(f_d) + sum(f_u)) f_t
           |    from ${bsNetTable}
           |    group by cAndSAndD, provid, nettype
           | ) t
         """.stripMargin
      val provNetResult = sqlContext.sql(provNetStatSQL)


      val cityStatSQL =
        s"""
           |select cAndSAndD, cityid,
           |       ma_sn, ma_rn, ma_rat,
           |       row_number() over(partition by cAndSAndD order by ma_rn desc ) ma_rank,
           |       a_sn, a_rn, a_rat,
           |       row_number() over(partition by cAndSAndD order by a_rn desc ) a_rank,
           |       f_d, f_u, f_t,
           |       row_number() over(partition by cAndSAndD order by f_t desc ) f_rank
           |from
           |(
           |    select cAndSAndD, cityid,
           |           sum(ma_sn) ma_sn, sum(ma_rn) ma_rn,  nvl(sum(ma_sn)/sum(ma_rn)) ma_rat,
           |           sum(a_sn) a_sn, sum(a_rn) a_rn,  nvl(sum(a_sn)/sum(a_rn)) a_rat,
           |           sum(f_d) f_d, sum(f_u) f_u, (sum(f_d) + sum(f_u)) f_t
           |    from ${bsNetTable}
           |    group by cAndSAndD, cityid
           | ) t
         """.stripMargin
      val cityResult = sqlContext.sql(cityStatSQL)

      val cityNetStatSQL =
        s"""
           |select cAndSAndD, cityid, cityname, nettype,
           |       ma_sn, ma_rn, ma_rat,
           |       row_number() over(partition by cAndSAndD, nettype order by ma_rn desc ) ma_rank,
           |       a_sn, a_rn, a_rat,
           |       row_number() over(partition by cAndSAndD, nettype order by a_rn desc ) a_rank,
           |       f_d, f_u, f_t,
           |       row_number() over(partition by cAndSAndD, nettype order by f_t desc ) f_rank
           |from
           |(
           |    select cAndSAndD, cityid, cityname, nettype,
           |           sum(ma_sn) ma_sn, sum(ma_rn) ma_rn,  nvl(sum(ma_sn)/sum(ma_rn)) ma_rat,
           |           sum(a_sn) a_sn, sum(a_rn) a_rn,  nvl(sum(a_sn)/sum(a_rn)) a_rat,
           |           sum(f_d) f_d, sum(f_u) f_u, (sum(f_d) + sum(f_u)) f_t
           |    from ${bsTable}
           |    group by cAndSAndD, cityid, cityname, nettype
           | ) t
         """.stripMargin
      val cityNetResult = sqlContext.sql(cityNetStatSQL)


      val bsStatSQL =
        s"""
           |select cAndSAndD, enbid, enbname,
           |       ma_sn, ma_rn, ma_rat,
           |       row_number() over(partition by cAndSAndD order by ma_rn desc ) ma_rank,
           |       a_sn, a_rn, a_rat,
           |       row_number() over(partition by cAndSAndD order by a_rn desc ) a_rank,
           |       f_d, f_u, f_t,
           |       row_number() over(partition by cAndSAndD order by f_t desc ) f_rank
           |from
           |(
           |    select cAndSAndD, enbid, enbname,
           |           sum(ma_sn) ma_sn, sum(ma_rn) ma_rn,  nvl(sum(ma_sn)/sum(ma_rn)) ma_rat,
           |           sum(a_sn) a_sn, sum(a_rn) a_rn,  nvl(sum(a_sn)/sum(a_rn)) a_rat,
           |           sum(f_d) f_d, sum(f_u) f_u, (sum(f_d) + sum(f_u)) f_t
           |    from ${bsTable}
           |    group by cAndSAndD, enbid, enbname
           | ) t
         """.stripMargin
      val bsResult = sqlContext.sql(bsStatSQL)

      val bsNetStatSQL =
        s"""
           |select cAndSAndD, enbid, enbname, nettype,
           |       ma_sn, ma_rn, ma_rat,
           |       row_number() over(partition by cAndSAndD, nettype order by ma_rn desc ) ma_rank,
           |       a_sn, a_rn, a_rat,
           |       row_number() over(partition by cAndSAndD, nettype order by a_rn desc ) a_rank,
           |       f_d, f_u, f_t,
           |       row_number() over(partition by cAndSAndD, nettype order by f_t desc ) f_rank
           |from
           |(
           |    select cAndSAndD, enbid, enbname, nettype,
           |           sum(ma_sn) ma_sn, sum(ma_rn) ma_rn,  nvl(sum(ma_sn)/sum(ma_rn)) ma_rat,
           |           sum(a_sn) a_sn, sum(a_rn) a_rn,  nvl(sum(a_sn)/sum(a_rn)) a_rat,
           |           sum(f_d) f_d, sum(f_u) f_u, (sum(f_d) + sum(f_u)) f_t
           |    from ${bsTable}
           |    group by cAndSAndD, enbid, enbname, nettype
           | ) t
         """.stripMargin
      val bsNetResult = sqlContext.sql(bsNetStatSQL)

      val bsNetStatRDD = bsNetResult.filter("f_rank<1000 or a_rank<1000 or f_rank<1000").coalesce(5).rdd.map(x => {
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




      // 更新时间, 断点时间比数据时间多1分钟
      val updateTime = DateUtils.timeCalcWithFormatConvertSafe(dataTime, "yyyyMMddHHmm", 1 * 60, "yyyyMMddHHmm")
      val analyzeColumn = if (progRunType == "0") "analyze_guess_bptime" else "analyze_real_bptime"
      HbaseUtils.upSertColumnByRowkey(analyzeBPHtable, "bp", "mme", analyzeColumn, updateTime)



    }

  }
}
