package com.zyuc.stat.iot.analysis.realtime

import com.zyuc.stat.iot.analysis.util.{AreaBaseHtableConverter, AreaHtableConverter, HbaseDataUtil}
import com.zyuc.stat.properties.ConfigProperties
import com.zyuc.stat.utils.{DateUtils, HbaseUtils}
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{Logging, SparkConf, SparkContext}

/**
  * 认证日志实时分析
  *
  * @author zhoucw
  * @version 1.0
  *
  */
object AreaRealTimeAnalysis extends Logging {

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
    val appName = sc.getConf.get("spark.app.name", "name_201710311340") // name_201708010040

    val resultBSHtable = sc.getConf.get("spark.app.htable.resultBSHtable", "analyze_summ_rst_bs")
    val areaRankHtable = sc.getConf.get("spark.app.htable.areaRankHtable", "analyze_summ_rst_area_rank")
    val resultBSBaseLineHtable = sc.getConf.get("spark.app.htable.resultBSBaseLineHtable", "analyze_summ_rst_bs_baseline")
    val bs4gTable = sc.getConf.get("spark.app.table.bs4gTable", "iot_basestation_4g")
    val bs3gTable = sc.getConf.get("spark.app.table.bs4gTable", "iot_basestation_3g")

    val analyzeBPHtable = sc.getConf.get("spark.app.htable.analyzeBPHtable", "analyze_bp_tab")

    val dataTime = appName.substring(appName.lastIndexOf("_") + 1)


    def areaAnalysis(analyType: String): Unit = {

      val bsTable = "bsTable_" + dataTime
      var bsSQL = "";

      if (analyType == "d") {

        val baseDataTime = HbaseUtils.getCloumnValueByRowkey("iot_dynamic_data", "area_basetime", "basic", "area_base_datatime") // 从habase里面获取
        val baseTime = sc.getConf.get("spark.app.baseDataTime", baseDataTime)
        val baseRowStart = baseTime
        val baseRowEnd = DateUtils.timeCalcWithFormatConvertSafe(baseTime, "yyyyMMddHHmm", 5 * 60, "yyyyMMddHHmm") // 201710272355_3g_P100000001_-1_-1_360000010091
        val hbaseBaseDF = AreaBaseHtableConverter.convertToDF1(sc, sqlContext, resultBSBaseLineHtable, (baseRowStart, baseRowEnd))
        val baseTmpTable = "baseTmpTable_" + dataTime
        hbaseBaseDF.registerTempTable(baseTmpTable)

        // 基线数据之后的rowkey范围
        val hCurRowStart = DateUtils.timeCalcWithFormatConvertSafe(baseTime, "yyyyMMddHHmm", 5 * 60, "yyyyMMddHHmm")
        val hCurRowEnd = DateUtils.timeCalcWithFormatConvertSafe(dataTime, "yyyyMMddHHmm", 5 * 60, "yyyyMMddHHmm")
        val hbaseCurDF = AreaHtableConverter.convertToDF(sc, sqlContext, resultBSHtable, (hCurRowStart, hCurRowEnd))
        val tmpCurHtable = "tmpCurHtable_" + dataTime
        hbaseCurDF.registerTempTable(tmpCurHtable)
        bsSQL =
          s"""
             |cache table ${bsTable} as
             |select cAndSAndD, nettype, provid, provname, cityid, cityname, enbid, enbname,
             |       sum(ma_sn) as ma_sn, sum(ma_rn) as ma_rn, sum(a_sn) as a_sn, sum(a_rn) as a_rn,
             |       sum(f_d) as f_d, sum(f_u) as f_u, sum(f_t) as f_t, sum(o_ln) as o_ln, sum(o_fln) as o_fln
             |from
             |(
             |    select  h.cAndSAndD, '3g' nettype, nvl(b.provcode, '0') provid, nvl(b.provname, '0') provname,
             |            nvl(b.citycode, '0') cityid, nvl(b.cityname, '0') cityname,
             |            nvl(h.enbid, '0') enbid, nvl(concat_ws('_', h.enbid, b.cityname), h.enbid)  enbname,
             |            h.ma_sn, h.ma_rn, h.a_sn, h.a_rn, h.a_fn, h.a_fcn, h.f_d, h.f_u, (h.f_d + h.f_u) f_t, h.o_ln, h.o_fln
             |    from    ${tmpCurHtable} h left join ${bs3gTable} b on(substr(h.enbid, 1, 4) = b.bsidpre)
             |    where   h.nettype='3g'
             |    union all
             |    select  h.cAndSAndD, '4g' nettype, nvl(b.provid, '0') provid, nvl(b.provname, '0') provname,
             |            nvl(b.cityid, '0') cityid, nvl(b.cityname, '0') cityname,
             |            nvl(b.enbid, h.enbid) enbid, nvl(b.zhlabel,h.enbid) enbname,
             |            h.ma_sn, h.ma_rn, h.a_sn, h.a_rn, h.a_fn, h.a_fcn, h.f_d, h.f_u, (h.f_d + h.f_u) f_t, h.o_ln, h.o_fln
             |    from    ${tmpCurHtable} h left join ${bs4gTable} b on(h.enbid = b.enbid)
             |    where  h.nettype='4g'
             |    union all
             |    select  cAndSAndD, nettype, provid, provname, cityid, cityname, enbid, enbname,
             |            ma_sn, ma_rn, a_sn, a_rn, (a_rn - a_sn) as a_fn, 0 as a_fcn, f_d, f_u,  (f_d + f_u) f_t, o_ln, o_fln
             |    from    ${baseTmpTable}
             |) t
             |group by cAndSAndD, nettype, provid, provname, cityid, cityname, enbid, enbname
       """.stripMargin
      } else if (analyType == "h") {
        // datatime一个小时前的rowkey范围
        val hCurRowStart = DateUtils.timeCalcWithFormatConvertSafe(dataTime, "yyyyMMddHHmm", -60 * 60, "yyyyMMddHHmm")
        val hCurRowEnd = DateUtils.timeCalcWithFormatConvertSafe(dataTime, "yyyyMMddHHmm", 5 * 60, "yyyyMMddHHmm")
        val hbaseCurDF = AreaHtableConverter.convertToDF(sc, sqlContext, resultBSHtable, (hCurRowStart, hCurRowEnd))
        val tmpCurHtable = "tmpCurHtable_" + dataTime
        hbaseCurDF.registerTempTable(tmpCurHtable)

        bsSQL =
          s"""
             |cache table ${bsTable} as
             |select cAndSAndD, nettype, provid, provname, cityid, cityname, enbid, enbname,
             |       sum(ma_sn) as ma_sn, sum(ma_rn) as ma_rn, sum(a_sn) as a_sn, sum(a_rn) as a_rn,
             |       sum(f_d) as f_d, sum(f_u) as f_u, sum(f_t) as f_t, sum(o_ln) as o_ln, sum(o_fln) as o_fln
             |from
             |(
             |    select  h.cAndSAndD, '3g' nettype, nvl(b.provcode, '0') provid, nvl(b.provname, '0') provname,
             |            nvl(b.citycode, '0') cityid, nvl(b.cityname, '0') cityname,
             |            nvl(h.enbid, '0') enbid, nvl(concat_ws('_', h.enbid, b.cityname), '0')  enbname,
             |            h.ma_sn, h.ma_rn, h.a_sn, h.a_rn, h.a_fn, h.a_fcn, h.f_d, h.f_u, (h.f_d + h.f_u) f_t, h.o_ln, h.o_fln
             |    from    ${tmpCurHtable} h left join ${bs3gTable} b on(substr(h.enbid, 1, 4) = b.bsidpre)
             |    where   h.nettype='3g'
             |    union all
             |    select  h.cAndSAndD, '4g' nettype, nvl(b.provid, '0') provid, nvl(b.provname, '0') provname,
             |            nvl(b.cityid, '0') cityid, nvl(b.cityname, '0') cityname,
             |            h.enbid, nvl(b.zhlabel,'0') enbname,
             |            h.ma_sn, h.ma_rn, h.a_sn, h.a_rn, h.a_fn, h.a_fcn, h.f_d, h.f_u, (h.f_d + h.f_u) f_t, h.o_ln, h.o_fln
             |    from    ${tmpCurHtable} h left join ${bs4gTable} b on(h.enbid = b.enbid)
             |    where  h.nettype='4g'
             |) t
             |group by cAndSAndD, nettype, provid, provname, cityid, cityname, enbid, enbname
       """.stripMargin
      }
      else if (analyType == "m5") {
        // datatime 5分钟前的rowkey范围
        val hCurRowStart = DateUtils.timeCalcWithFormatConvertSafe(dataTime, "yyyyMMddHHmm", -5 * 60, "yyyyMMddHHmm")
        val hCurRowEnd = DateUtils.timeCalcWithFormatConvertSafe(dataTime, "yyyyMMddHHmm", 5 * 60, "yyyyMMddHHmm")
        val hbaseCurDF = AreaHtableConverter.convertToDF(sc, sqlContext, resultBSHtable, (hCurRowStart, hCurRowEnd))
        val tmpCurHtable = "tmpCurHtable_" + dataTime
        hbaseCurDF.registerTempTable(tmpCurHtable)

        bsSQL =
          s"""
             |cache table ${bsTable} as
             |select cAndSAndD, nettype, provid, provname, cityid, cityname, enbid, enbname,
             |       sum(ma_sn) as ma_sn, sum(ma_rn) as ma_rn, sum(a_sn) as a_sn, sum(a_rn) as a_rn,
             |       sum(f_d) as f_d, sum(f_u) as f_u, sum(f_t) as f_t, sum(o_ln) as o_ln, sum(o_fln) as o_fln
             |from
             |(
             |    select  h.cAndSAndD, '3g' nettype, nvl(b.provcode, '0') provid, nvl(b.provname, '0') provname,
             |            nvl(b.citycode, '0') cityid, nvl(b.cityname, '0') cityname,
             |            nvl(h.enbid, '0') enbid, nvl(concat_ws('_', h.enbid, b.cityname), '0')  enbname,
             |            h.ma_sn, h.ma_rn, h.a_sn, h.a_rn, h.a_fn, h.a_fcn, h.f_d, h.f_u, (h.f_d + h.f_u) f_t, h.o_ln, h.o_fln
             |    from    ${tmpCurHtable} h left join ${bs3gTable} b on(substr(h.enbid, 1, 4) = b.bsidpre)
             |    where   h.nettype='3g'
             |    union all
             |    select  h.cAndSAndD, '4g' nettype, nvl(b.provid, '0') provid, nvl(b.provname, '0') provname,
             |            nvl(b.cityid, '0') cityid, nvl(b.cityname, '0') cityname,
             |            h.enbid, nvl(b.zhlabel,'0') enbname,
             |            h.ma_sn, h.ma_rn, h.a_sn, h.a_rn, h.a_fn, h.a_fcn, h.f_d, h.f_u, (h.f_d + h.f_u) f_t, h.o_ln, h.o_fln
             |    from    ${tmpCurHtable} h left join ${bs4gTable} b on(h.enbid = b.enbid)
             |    where  h.nettype='4g'
             |) t
             |group by cAndSAndD, nettype, provid, provname, cityid, cityname, enbid, enbname
       """.stripMargin
      }

      sqlContext.sql(bsSQL)

      // 由于3g和4g基站的编码不会重复， 所以可以采用3/4/t一块入hbase
      val bsNetStatSQL =
        s"""
           |select provname, cAndSAndD, enbid, enbname, nettype,
           |       ma_sn, ma_rn, if(ma_rn>'0',ma_sn/ma_rn, 0) ma_rat,
           |       a_sn, a_rn, if(a_rn>'0', a_sn/a_rn, 0) a_net_rat,
           |       f_d, f_u, f_t, o_ln, o_fln,
           |       row_number() over(partition by cAndSAndD, nettype order by ma_rn desc ) ma_net_rank,
           |       row_number() over(partition by cAndSAndD, nettype order by a_rn desc ) a_net_rank,
           |       row_number() over(partition by cAndSAndD, nettype order by f_t desc ) f_net_rank,
           |       row_number() over(partition by cAndSAndD  order by f_t desc ) f_total_rank,
           |       row_number() over(partition by cAndSAndD, nettype order by o_fln desc ) o_net_rank
           |from ${bsTable} t
         """.stripMargin
      val bsNetResult = sqlContext.sql(bsNetStatSQL)

      val bsNetStatRDD = bsNetResult.coalesce(10).rdd.map(x => {
        val maNetRank = if (null == x(16)) "0" else String.format("%3s", x(16).toString).replaceAll(" ", "0")
        val aNetRank = if (null == x(17)) "0" else String.format("%3s", x(17).toString).replaceAll(" ", "0")
        val fNetRank = if (null == x(18)) "0" else String.format("%3s", x(18).toString).replaceAll(" ", "0")
        val fTotalRank = if (null == x(19)) "0" else String.format("%3s", x(19).toString).replaceAll(" ", "0")
        val oTotalRank = if (null == x(20)) "0" else String.format("%3s", x(20).toString).replaceAll(" ", "0")

        val proName = if (null == x(0)) "其他" else x(0).toString
        val csd = if (null == x(1)) "0" else x(1).toString
        val maRow = x(1).toString + "_" + maNetRank
        val aRow = x(1).toString + "_" + aNetRank
        val fRow = x(1).toString + "_" + fNetRank
        val oRow = x(1).toString + "_" + oTotalRank

        val bid = if (null == x(2)) "0" else x(2).toString
        val bname = if (null == x(3)) "0" else x(3).toString
        val net = if (null == x(4)) "0" else x(4).toString
        val netFlag = if ("3g" == net) "3" else if ("4g" == net) "4" else net
        val marsn = if (null == x(5)) "0" else x(5).toString
        val marn = if (null == x(6)) "0" else x(6).toString
        val marat = if (null == x(7)) "0" else x(7).toString

        val arsn = if (null == x(8)) "0" else x(8).toString
        val arn = if (null == x(9)) "0" else x(9).toString
        val arat = if (null == x(10)) "0" else x(10).toString

        val fd = if (null == x(11)) "0" else x(11).toString
        val fu = if (null == x(12)) "0" else x(12).toString
        val ft = if (null == x(13)) "0" else x(13).toString

        val ofln = if (null == x(14)) "0" else x(14).toString

        val maNetPut = if (maNetRank.toDouble < 1000) new Put(Bytes.toBytes(maRow)) else null
        val aNetPut = if (aNetRank.toDouble < 1000) new Put(Bytes.toBytes(aRow)) else null
        val fNetPut = if (fNetRank.toDouble < 1000) new Put(Bytes.toBytes(fRow)) else null
        val fTotalPut = if (fTotalRank.toDouble < 1000) new Put(Bytes.toBytes(fRow)) else null
        val oTotalPut = if (oTotalRank.toDouble < 1000) new Put(Bytes.toBytes(oRow)) else null

        var maTuple:Tuple2[ImmutableBytesWritable, Put] = null
        var authTuple:Tuple2[ImmutableBytesWritable, Put] = null
        var fNetTuple:Tuple2[ImmutableBytesWritable, Put] = null
        var ftTuple:Tuple2[ImmutableBytesWritable, Put] = null
        var onlineTuple:Tuple2[ImmutableBytesWritable, Put] = null

        if (maNetPut !=null && marn >= "1") {
          maNetPut.addColumn(Bytes.toBytes("r"), Bytes.toBytes("ma_" + analyType + "_t_rn_bd"), Bytes.toBytes(bid))
          maNetPut.addColumn(Bytes.toBytes("r"), Bytes.toBytes("ma_" + analyType + "_t_rn_bn"), Bytes.toBytes(bname))
          maNetPut.addColumn(Bytes.toBytes("r"), Bytes.toBytes("ma_" + analyType + "_t_rn_bv"), Bytes.toBytes(marn))
          maNetPut.addColumn(Bytes.toBytes("r"), Bytes.toBytes("ma_" + analyType + "_t_rat_bd"), Bytes.toBytes(bid))
          maNetPut.addColumn(Bytes.toBytes("r"), Bytes.toBytes("ma_" + analyType + "_t_rat_bn"), Bytes.toBytes(bname))
          maNetPut.addColumn(Bytes.toBytes("r"), Bytes.toBytes("ma_" + analyType + "_t_rat_bv"), Bytes.toBytes(marat))
          maTuple = (new ImmutableBytesWritable, maNetPut)
        }

        if (aNetPut != null &&  arn >= "1") {
          aNetPut.addColumn(Bytes.toBytes("r"), Bytes.toBytes("a_" + analyType + "_t_rn_bd"), Bytes.toBytes(bid))
          aNetPut.addColumn(Bytes.toBytes("r"), Bytes.toBytes("a_" + analyType + "_t_rn_bn"), Bytes.toBytes(bname))
          aNetPut.addColumn(Bytes.toBytes("r"), Bytes.toBytes("a_" + analyType + "_t_rn_bv"), Bytes.toBytes(arn))
          aNetPut.addColumn(Bytes.toBytes("r"), Bytes.toBytes("a_" + analyType + "_t_rat_bd"), Bytes.toBytes(bid))
          aNetPut.addColumn(Bytes.toBytes("r"), Bytes.toBytes("a_" + analyType + "_t_rat_bn"), Bytes.toBytes(bname))
          aNetPut.addColumn(Bytes.toBytes("r"), Bytes.toBytes("a_" + analyType + "_t_rat_bv"), Bytes.toBytes(arat))
          authTuple = (new ImmutableBytesWritable, aNetPut)
        }

        if (fNetPut != null &&  ft >= "1") {
          fNetPut.addColumn(Bytes.toBytes("r"), Bytes.toBytes("f_" + analyType + "_" + netFlag + "_t_bd"), Bytes.toBytes(bid))
          fNetPut.addColumn(Bytes.toBytes("r"), Bytes.toBytes("f_" + analyType + "_" + netFlag + "_t_bn"), Bytes.toBytes(bname))
          fNetPut.addColumn(Bytes.toBytes("r"), Bytes.toBytes("f_" + analyType + "_" + netFlag + "_t_bv"), Bytes.toBytes(ft))
          fNetTuple = (new ImmutableBytesWritable, fNetPut)
        }

        if (fTotalPut != null) {
          fTotalPut.addColumn(Bytes.toBytes("r"), Bytes.toBytes("f_" + analyType + "_t_t_bd"), Bytes.toBytes(bid))
          fTotalPut.addColumn(Bytes.toBytes("r"), Bytes.toBytes("f_" + analyType + "_t_t_bn"), Bytes.toBytes(bname))
          fTotalPut.addColumn(Bytes.toBytes("r"), Bytes.toBytes("f_" + analyType + "_t_t_bv"), Bytes.toBytes(ft))
          ftTuple = (new ImmutableBytesWritable, fTotalPut)
        }

        if (oTotalPut != null && ofln >= "1") {
          oTotalPut.addColumn(Bytes.toBytes("r"), Bytes.toBytes("o_" + analyType + "_t_ablo_bd"), Bytes.toBytes(bid))
          oTotalPut.addColumn(Bytes.toBytes("r"), Bytes.toBytes("o_" + analyType + "_t_ablo_bn"), Bytes.toBytes(bname))
          oTotalPut.addColumn(Bytes.toBytes("r"), Bytes.toBytes("o_" + analyType + "_t_ablo_bv"), Bytes.toBytes(ofln))
          onlineTuple = (new ImmutableBytesWritable, oTotalPut)
        }

        (maTuple, authTuple, fNetTuple, ftTuple, onlineTuple)

      })


      HbaseDataUtil.saveRddToHbase(areaRankHtable, bsNetStatRDD.filter(null != _._1).map(_._1))
      //HbaseDataUtil.saveRddToHbase(areaRankHtable, bsNetStatRDD.filter(null != _._2).map(_._2))
      HbaseDataUtil.saveRddToHbase(areaRankHtable, bsNetStatRDD.filter(null != _._3).map(_._3))
      HbaseDataUtil.saveRddToHbase(areaRankHtable, bsNetStatRDD.filter(null != _._4).map(_._4))
      HbaseDataUtil.saveRddToHbase(areaRankHtable, bsNetStatRDD.filter(null != _._5).map(_._5))





      val cityStatSQL =
        s"""select cAndSAndD, cityname, nettype,
           |       ma_sn, ma_rn, if(ma_rn>'0',ma_sn/ma_rn, 0) ma_rat,
           |       a_sn, a_rn, if(a_rn>'0',a_sn/a_rn, 0) a_rat,
           |       f_d, f_u, f_t, o_ln, o_fln,
           |       row_number() over(partition by cAndSAndD, nettype order by ma_rn desc) ma_rank,
           |       row_number() over(partition by cAndSAndD, nettype order by a_rn desc) a_rank,
           |       row_number() over(partition by cAndSAndD, nettype order by f_t desc) f_rank,
           |       row_number() over(partition by cAndSAndD, nettype order by o_fln desc) o_rank
           |from
           |(
           |    select cAndSAndD, nvl(nettype, 't') nettype, cityname,
           |           sum(ma_sn) as ma_sn, sum(ma_rn) as ma_rn, sum(a_sn) as a_sn, sum(a_rn) as a_rn,
           |           sum(f_d) as f_d, sum(f_u) as f_u, sum(f_t) as f_t, sum(o_ln) as o_ln, sum(o_fln) as o_fln
           |    from ${bsTable}
           |    group by cAndSAndD, nettype, cityname
           |    grouping sets((cAndSAndD, cityname), (cAndSAndD, nettype,  cityname))
           |) t
       """.stripMargin

      val cityResultDF = sqlContext.sql(cityStatSQL)
      val cityStatRDD = cityResultDF.coalesce(10).rdd.map(x => {
        val maRank = if (null == x(14)) "0" else String.format("%3s", x(14).toString).replaceAll(" ", "0")
        val aRank = if (null == x(15)) "0" else String.format("%3s", x(15).toString).replaceAll(" ", "0")
        val fRank = if (null == x(16)) "0" else String.format("%3s", x(16).toString).replaceAll(" ", "0")
        val oRank = if (null == x(17)) "0" else String.format("%3s", x(17).toString).replaceAll(" ", "0")

        val csd = if (null == x(0)) "0" else x(0).toString
        val maRow = csd + "_" + maRank
        val aRow = csd + "_" + aRank
        val fRow = csd + "_" + fRank
        val oRow = csd + "_" + oRank

        val cname = if (null == x(1)) "0" else x(1).toString
        val net = if (null == x(2)) "0" else x(2).toString
        val netFlag = if ("3g" == net) "3" else if ("4g" == net) "4" else net
        val marsn = if (null == x(3)) "0" else x(3).toString
        val marn = if (null == x(4)) "0" else x(4).toString
        val marat = if (null == x(5)) "0" else x(5).toString

        val arsn = if (null == x(6)) "0" else x(6).toString
        val arn = if (null == x(7)) "0" else x(7).toString
        val arat = if (null == x(8)) "0" else x(8).toString

        val fd = if (null == x(9)) "0" else x(9).toString
        val fu = if (null == x(10)) "0" else x(10).toString
        val ft = if (null == x(11)) "0" else x(11).toString

        val ofln = if (null == x(13)) "0" else x(13).toString

        val maPut = if (maRank.toDouble < 1000) new Put(Bytes.toBytes(maRow)) else null
        val aPut = if (aRank.toDouble < 1000) new Put(Bytes.toBytes(aRow)) else null
        val fPut = if (fRank.toDouble < 1000) new Put(Bytes.toBytes(fRow)) else null
        val oPut = if (oRank.toDouble < 1000) new Put(Bytes.toBytes(oRow)) else null


        var maTuple:Tuple2[ImmutableBytesWritable, Put] = null
        var authTuple:Tuple2[ImmutableBytesWritable, Put] = null
        var ftTuple:Tuple2[ImmutableBytesWritable, Put] = null
        var onlineTuple:Tuple2[ImmutableBytesWritable, Put] = null

        if (maPut != null && marn >= "1") {
          maPut.addColumn(Bytes.toBytes("r"), Bytes.toBytes("ma_" + analyType + "_" + netFlag + "_rn_rd"), Bytes.toBytes(cname))
          maPut.addColumn(Bytes.toBytes("r"), Bytes.toBytes("ma_" + analyType + "_" + netFlag + "_rn_rn"), Bytes.toBytes(cname))
          maPut.addColumn(Bytes.toBytes("r"), Bytes.toBytes("ma_" + analyType + "_" + netFlag + "_rn_rv"), Bytes.toBytes(marn))
          maPut.addColumn(Bytes.toBytes("r"), Bytes.toBytes("ma_" + analyType + "_" + netFlag + "_rat_rd"), Bytes.toBytes(cname))
          maPut.addColumn(Bytes.toBytes("r"), Bytes.toBytes("ma_" + analyType + "_" + netFlag + "_rat_rn"), Bytes.toBytes(cname))
          maPut.addColumn(Bytes.toBytes("r"), Bytes.toBytes("ma_" + analyType + "_" + netFlag + "_rat_rv"), Bytes.toBytes(marat))

          maTuple = (new ImmutableBytesWritable, maPut)
        }

        if (aPut != null &&  arn >= "1") {
          aPut.addColumn(Bytes.toBytes("r"), Bytes.toBytes("a_" + analyType + "_" + netFlag + "_rn_rd"), Bytes.toBytes(cname))
          aPut.addColumn(Bytes.toBytes("r"), Bytes.toBytes("a_" + analyType + "_" + netFlag + "_rn_rn"), Bytes.toBytes(cname))
          aPut.addColumn(Bytes.toBytes("r"), Bytes.toBytes("a_" + analyType + "_" + netFlag + "_rn_rv"), Bytes.toBytes(arn))
          aPut.addColumn(Bytes.toBytes("r"), Bytes.toBytes("a_" + analyType + "_" + netFlag + "_rat_rd"), Bytes.toBytes(cname))
          aPut.addColumn(Bytes.toBytes("r"), Bytes.toBytes("a_" + analyType + "_" + netFlag + "_rat_rn"), Bytes.toBytes(cname))
          aPut.addColumn(Bytes.toBytes("r"), Bytes.toBytes("a_" + analyType + "_" + netFlag + "_rat_rv"), Bytes.toBytes(arat))
          authTuple = (new ImmutableBytesWritable, aPut)
        }

        if (fPut != null &&  ft >= "1") {
          fPut.addColumn(Bytes.toBytes("r"), Bytes.toBytes("f_" + analyType + "_" + netFlag + "_t_rd"), Bytes.toBytes(cname))
          fPut.addColumn(Bytes.toBytes("r"), Bytes.toBytes("f_" + analyType + "_" + netFlag + "_t_rn"), Bytes.toBytes(cname))
          fPut.addColumn(Bytes.toBytes("r"), Bytes.toBytes("f_" + analyType + "_" + netFlag + "_t_rv"), Bytes.toBytes(ft))
          ftTuple = (new ImmutableBytesWritable, fPut)
        }


        if (oPut != null && ofln >= "1") {
          oPut.addColumn(Bytes.toBytes("r"), Bytes.toBytes("o_" + analyType + "_" + netFlag + "_ablo_rd"), Bytes.toBytes(cname))
          oPut.addColumn(Bytes.toBytes("r"), Bytes.toBytes("o_" + analyType + "_" + netFlag + "_ablo_rn"), Bytes.toBytes(cname))
          oPut.addColumn(Bytes.toBytes("r"), Bytes.toBytes("o_" + analyType + "_" + netFlag + "_ablo_rv"), Bytes.toBytes(ofln))
          onlineTuple = (new ImmutableBytesWritable, oPut)
        }

        (maTuple, authTuple, ftTuple, onlineTuple)

      })

      HbaseDataUtil.saveRddToHbase(areaRankHtable, cityStatRDD.filter(null != _._1).map(_._1))
      //HbaseDataUtil.saveRddToHbase(areaRankHtable, cityStatRDD.filter(null != _._2).map(_._2))
      HbaseDataUtil.saveRddToHbase(areaRankHtable, cityStatRDD.filter(null != _._3).map(_._3))
      HbaseDataUtil.saveRddToHbase(areaRankHtable, cityStatRDD.filter(null != _._4).map(_._4))


      val proStatSQL =
        s"""
           |select cAndSAndD, provname, nettype,
           |       ma_sn, ma_rn,  if(ma_rn>'0',ma_sn/ma_rn, 0) ma_rat,
           |       a_sn, a_rn, if(a_rn>'0',a_sn/a_rn, 0) a_rat,
           |       f_d, f_u, f_t, o_ln, o_fln,
           |       row_number() over(partition by cAndSAndD, nettype order by ma_rn desc) ma_rank,
           |       row_number() over(partition by cAndSAndD, nettype order by a_rn desc) a_rank,
           |       row_number() over(partition by cAndSAndD, nettype order by f_t desc) f_rank,
           |       row_number() over(partition by cAndSAndD, nettype order by o_fln desc) o_rank
           |from
           |(
           |    select cAndSAndD, nvl(nettype, 't') nettype, provname,
           |           sum(ma_sn) as ma_sn, sum(ma_rn) as ma_rn, sum(a_sn) as a_sn, sum(a_rn) as a_rn,
           |           sum(f_d) as f_d, sum(f_u) as f_u, sum(f_t) as f_t, sum(o_ln) as o_ln, sum(o_fln) as o_fln
           |    from ${bsTable}
           |    group by cAndSAndD, nettype, provname
           |    grouping sets((cAndSAndD, provname), (cAndSAndD, nettype,  provname))
           |) t
       """.stripMargin

      val proResultDF = sqlContext.sql(proStatSQL)
      val proStatRDD = proResultDF.coalesce(10).rdd.map(x => {
        val maRank = if (null == x(14)) "0" else String.format("%3s", x(14).toString).replaceAll(" ", "0")
        val aRank = if (null == x(15)) "0" else String.format("%3s", x(15).toString).replaceAll(" ", "0")
        val fRank = if (null == x(16)) "0" else String.format("%3s", x(16).toString).replaceAll(" ", "0")
        val oRank = if (null == x(17)) "0" else String.format("%3s", x(17).toString).replaceAll(" ", "0")

        val csd = if (null == x(0)) "0" else x(0).toString
        val maRow = csd + "_" + maRank
        val aRow = csd + "_" + aRank
        val fRow = csd + "_" + fRank
        val oRow = csd + "_" + oRank

        val pname = if (null == x(1)) "0" else x(1).toString
        val net = if (null == x(2)) "0" else x(2).toString
        val netFlag = if ("3g" == net) "3" else if ("4g" == net) "4" else net
        val marsn = if (null == x(3)) "0" else x(3).toString
        val marn = if (null == x(4)) "0" else x(4).toString
        val marat = if (null == x(5)) "0" else x(5).toString

        val arsn = if (null == x(6)) "0" else x(6).toString
        val arn = if (null == x(7)) "0" else x(7).toString
        val arat = if (null == x(8)) "0" else x(8).toString

        val fd = if (null == x(9)) "0" else x(9).toString
        val fu = if (null == x(10)) "0" else x(10).toString
        val ft = if (null == x(11)) "0" else x(11).toString

        val ofln = if (null == x(13)) "0" else x(13).toString

        val maPut = if (maRank.toDouble < 1000) new Put(Bytes.toBytes(maRow)) else null
        val aPut = if (aRank.toDouble < 1000) new Put(Bytes.toBytes(aRow)) else null
        val fPut = if (fRank.toDouble < 1000) new Put(Bytes.toBytes(fRow)) else null
        val oPut = if (oRank.toDouble < 1000) new Put(Bytes.toBytes(oRow)) else null

        var maTuple:Tuple2[ImmutableBytesWritable, Put] = null
        var authTuple:Tuple2[ImmutableBytesWritable, Put] = null
        var ftTuple:Tuple2[ImmutableBytesWritable, Put] = null
        var onlineTuple:Tuple2[ImmutableBytesWritable, Put] = null

        if (maPut != null && marn >= "1") {
          maPut.addColumn(Bytes.toBytes("r"), Bytes.toBytes("ma_" + analyType + "_" + netFlag + "_rn_pd"), Bytes.toBytes(pname))
          maPut.addColumn(Bytes.toBytes("r"), Bytes.toBytes("ma_" + analyType + "_" + netFlag + "_rn_pn"), Bytes.toBytes(pname))
          maPut.addColumn(Bytes.toBytes("r"), Bytes.toBytes("ma_" + analyType + "_" + netFlag + "_rn_pv"), Bytes.toBytes(marn))
          maPut.addColumn(Bytes.toBytes("r"), Bytes.toBytes("ma_" + analyType + "_" + netFlag + "_rat_rd"), Bytes.toBytes(pname))
          maPut.addColumn(Bytes.toBytes("r"), Bytes.toBytes("ma_" + analyType + "_" + netFlag + "_rat_rn"), Bytes.toBytes(pname))
          maPut.addColumn(Bytes.toBytes("r"), Bytes.toBytes("ma_" + analyType + "_" + netFlag + "_rat_rv"), Bytes.toBytes(marat))
          maTuple = (new ImmutableBytesWritable, maPut)
        }

        if (aPut != null &&  arn >= "1") {
          aPut.addColumn(Bytes.toBytes("r"), Bytes.toBytes("a_" + analyType + "_" + netFlag + "_rn_pd"), Bytes.toBytes(pname))
          aPut.addColumn(Bytes.toBytes("r"), Bytes.toBytes("a_" + analyType + "_" + netFlag + "_rn_pn"), Bytes.toBytes(pname))
          aPut.addColumn(Bytes.toBytes("r"), Bytes.toBytes("a_" + analyType + "_" + netFlag + "_rn_pv"), Bytes.toBytes(arn))
          aPut.addColumn(Bytes.toBytes("r"), Bytes.toBytes("a_" + analyType + "_" + netFlag + "_rat_pd"), Bytes.toBytes(pname))
          aPut.addColumn(Bytes.toBytes("r"), Bytes.toBytes("a_" + analyType + "_" + netFlag + "_rat_pn"), Bytes.toBytes(pname))
          aPut.addColumn(Bytes.toBytes("r"), Bytes.toBytes("a_" + analyType + "_" + netFlag + "_rat_pv"), Bytes.toBytes(arat))
          authTuple = (new ImmutableBytesWritable, aPut)
        }

        if (fPut != null &&  ft >= "1") {
          fPut.addColumn(Bytes.toBytes("r"), Bytes.toBytes("f_" + analyType + "_" + netFlag + "_t_pd"), Bytes.toBytes(pname))
          fPut.addColumn(Bytes.toBytes("r"), Bytes.toBytes("f_" + analyType + "_" + netFlag + "_t_pn"), Bytes.toBytes(pname))
          fPut.addColumn(Bytes.toBytes("r"), Bytes.toBytes("f_" + analyType + "_" + netFlag + "_t_pv"), Bytes.toBytes(ft))
          ftTuple = (new ImmutableBytesWritable, fPut)
        }

        if (oPut != null && ofln >= "1") {
          oPut.addColumn(Bytes.toBytes("r"), Bytes.toBytes("o_" + analyType + "_" + netFlag + "_ablo_pd"), Bytes.toBytes(pname))
          oPut.addColumn(Bytes.toBytes("r"), Bytes.toBytes("o_" + analyType + "_" + netFlag + "_ablo_pn"), Bytes.toBytes(pname))
          oPut.addColumn(Bytes.toBytes("r"), Bytes.toBytes("o_" + analyType + "_" + netFlag + "_ablo_pv"), Bytes.toBytes(ofln))
          onlineTuple = (new ImmutableBytesWritable, oPut)
        }

        (maTuple, authTuple, ftTuple, onlineTuple)

      })

      HbaseDataUtil.saveRddToHbase(areaRankHtable, proStatRDD.filter(null != _._1).map(_._1))
      //HbaseDataUtil.saveRddToHbase(areaRankHtable, proStatRDD.filter(null != _._2).map(_._2))
      HbaseDataUtil.saveRddToHbase(areaRankHtable, proStatRDD.filter(null != _._3).map(_._3))
      HbaseDataUtil.saveRddToHbase(areaRankHtable, proStatRDD.filter(null != _._4).map(_._4))


      // 汇总
      val sumStatSQL =
        s"""
           |select cAndSAndD, nettype,
           |       ma_sn, ma_rn,  if(ma_rn>'0',ma_sn/ma_rn, 0) ma_rat,
           |       a_sn, a_rn, if(a_rn>'0',a_sn/a_rn, 0) a_rat,
           |       f_d, f_u, f_t, o_ln, o_fln,
           |       row_number() over(partition by cAndSAndD, nettype order by ma_rn desc) ma_rank,
           |       row_number() over(partition by cAndSAndD, nettype order by a_rn desc) a_rank,
           |       row_number() over(partition by cAndSAndD, nettype order by f_t desc) f_rank
           |from
           |(
           |    select cAndSAndD, nvl(nettype, 't') nettype,
           |           sum(ma_sn) as ma_sn, sum(ma_rn) as ma_rn, sum(a_sn) as a_sn, sum(a_rn) as a_rn,
           |           sum(f_d) as f_d, sum(f_u) as f_u, sum(f_t) as f_t, sum(o_ln) as o_ln, sum(o_fln) as o_fln
           |    from ${bsTable}
           |    group by cAndSAndD, nettype
           |    grouping sets(cAndSAndD, (cAndSAndD, nettype))
           |) t
       """.stripMargin

      val sumResultDF = sqlContext.sql(sumStatSQL)
      val sumStatRDD = sumResultDF.coalesce(10).rdd.map(x => {
        val maRank = if (null == x(13)) "0" else String.format("%3s", x(13).toString).replaceAll(" ", "0")
        val aRank = if (null == x(14)) "0" else String.format("%3s", x(14).toString).replaceAll(" ", "0")
        val fRank = if (null == x(15)) "0" else String.format("%3s", x(15).toString).replaceAll(" ", "0")
        val oRank = if (null == x(16)) "0" else String.format("%3s", x(16).toString).replaceAll(" ", "0")



        val csd = if (null == x(0)) "0" else x(0).toString
        val maRow = csd + "_000"
        val aRow = csd + "_000"
        val fRow = csd + "_000"
        val oRow = csd + "_000"

        val net = if (null == x(1)) "0" else x(1).toString
        val netFlag = if ("3g" == net) "3" else if ("4g" == net) "4" else net
        val marsn = if (null == x(2)) "0" else x(2).toString
        val marn = if (null == x(3)) "0" else x(3).toString
        val marat = if (null == x(4)) "0" else x(4).toString

        val arsn = if (null == x(5)) "0" else x(5).toString
        val arn = if (null == x(6)) "0" else x(6).toString
        val arat = if (null == x(7)) "0" else x(7).toString

        val fd = if (null == x(8)) "0" else x(8).toString
        val fu = if (null == x(9)) "0" else x(9).toString
        val ft = if (null == x(10)) "0" else x(10).toString

        val ofln = if (null == x(12)) "0" else x(12).toString

        val maPut = if (maRank.toDouble < 1000) new Put(Bytes.toBytes(maRow)) else null
        val aPut = if (aRank.toDouble < 1000) new Put(Bytes.toBytes(aRow)) else null
        val fPut = if (fRank.toDouble < 1000) new Put(Bytes.toBytes(fRow)) else null
        val oPut = if (oRank.toDouble < 1000) new Put(Bytes.toBytes(oRow)) else null

        var maTuple:Tuple2[ImmutableBytesWritable, Put] = null
        var authTuple:Tuple2[ImmutableBytesWritable, Put] = null
        var ftTuple:Tuple2[ImmutableBytesWritable, Put] = null
        var onlineTuple:Tuple2[ImmutableBytesWritable, Put] = null

        if (maPut != null && marn >= "1") {
          maPut.addColumn(Bytes.toBytes("r"), Bytes.toBytes("ma_" + analyType + "_" + netFlag + "_rn_sv"), Bytes.toBytes(marn))
          maPut.addColumn(Bytes.toBytes("r"), Bytes.toBytes("ma_" + analyType + "_" + netFlag + "_rat_sv"), Bytes.toBytes(marat))

          maTuple = (new ImmutableBytesWritable, maPut)
        }

        if (aPut != null &&  arn  >= "1") {
          aPut.addColumn(Bytes.toBytes("r"), Bytes.toBytes("a_" + analyType + "_" + netFlag + "_rn_sv"), Bytes.toBytes(arn))
          aPut.addColumn(Bytes.toBytes("r"), Bytes.toBytes("a_" + analyType + "_" + netFlag + "_rat_sv"), Bytes.toBytes(arat))
          authTuple = (new ImmutableBytesWritable, aPut)
        }

        if (fPut != null &&  ft >= "1") {
          fPut.addColumn(Bytes.toBytes("r"), Bytes.toBytes("f_" + analyType + "_" + netFlag + "_t_sv"), Bytes.toBytes(ft))
          ftTuple = (new ImmutableBytesWritable, fPut)
        }


        if (oPut != null && ofln >= "1") {
          oPut.addColumn(Bytes.toBytes("r"), Bytes.toBytes("o_" + analyType + "_" + netFlag + "_ablo_sv"), Bytes.toBytes(ofln))
          onlineTuple = (new ImmutableBytesWritable, oPut)
        }

        (maTuple, authTuple, ftTuple, ftTuple)

      })

      HbaseDataUtil.saveRddToHbase(areaRankHtable, proStatRDD.filter(null != _._1).map(_._1))
      //HbaseDataUtil.saveRddToHbase(areaRankHtable, proStatRDD.filter(null != _._2).map(_._2))
      HbaseDataUtil.saveRddToHbase(areaRankHtable, proStatRDD.filter(null != _._3).map(_._3))
      HbaseDataUtil.saveRddToHbase(areaRankHtable, proStatRDD.filter(null != _._4).map(_._4))

    }

    // auth 5 minutes
    AuthAreaAnalysis.AuthAreaAnalysis(sc, sqlContext, "m5", dataTime, resultBSHtable, areaRankHtable, bs3gTable, bs4gTable)

    areaAnalysis("h")
    areaAnalysis("d")


    // 更新时间, 断点时间比数据时间多1分钟
    val updateTime = DateUtils.timeCalcWithFormatConvertSafe(dataTime, "yyyyMMddHHmm", 1*60, "yyyyMMddHHmm")
    HbaseUtils.upSertColumnByRowkey(analyzeBPHtable, "bp", "rank_byArea_bptime", "rank_byArea_bptime", updateTime)
  }
}
