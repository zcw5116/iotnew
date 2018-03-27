package com.zyuc.stat.iot.analysis.realtime

import com.zyuc.stat.iot.analysis.util.{AreaBaseHtableConverter, AreaHtableConverter, HbaseDataUtil}
import com.zyuc.stat.properties.ConfigProperties
import com.zyuc.stat.utils.{DateUtils, HbaseUtils}
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{Logging, SparkConf, SparkContext}

/**
  * 认证日志实时分析
  *
  * @author zhoucw
  * @version 1.0
  *
  */
object AuthAreaAnalysis extends Logging {

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

  }

  def AuthAreaAnalysis(sc:SparkContext, sqlContext: SQLContext, analyType: String, dataTime:String, resultBSHtable:String, areaRankHtable:String, bs3gTable:String, bs4gTable:String): Unit = {

    val bsTable = "bsTable_" + dataTime
    var bsSQL = ""


    // datatime一个小时前的rowkey范围
    val hCurRowStart = DateUtils.timeCalcWithFormatConvertSafe(dataTime, "yyyyMMddHHmm", -5 * 60, "yyyyMMddHHmm")
    val hCurRowEnd = DateUtils.timeCalcWithFormatConvertSafe(dataTime, "yyyyMMddHHmm", 0 * 60, "yyyyMMddHHmm")
    val hbaseCurDF = AreaHtableConverter.convertToDF(sc, sqlContext, resultBSHtable, (hCurRowStart, hCurRowEnd))
    val tmpCurHtable = "tmpCurHtable_" + dataTime
    hbaseCurDF.registerTempTable(tmpCurHtable)

    bsSQL =
      s"""
         |cache table ${bsTable} as
         |select cAndSAndD, nettype, provid, provname, cityid, cityname, enbid, enbname,
         |       a_sn, a_rn, a_fn, a_rcn, a_fcn
         |from
         |(
         |    select  h.cAndSAndD, '3g' nettype, nvl(b.provcode, '0') provid, nvl(b.provname, '0') provname,
         |            nvl(b.citycode, '0') cityid, nvl(b.cityname, '0') cityname,
         |            nvl(h.enbid, '0') enbid, nvl(concat_ws('_', h.enbid, b.cityname), '0')  enbname,
         |            h.a_sn, h.a_rn, h.a_fn, h.a_rcn, h.a_fcn
         |    from    ${tmpCurHtable} h left join ${bs3gTable} b on(substr(h.enbid, 1, 4) = b.bsidpre)
         |    where   h.nettype='3g'
         |    union all
         |    select  h.cAndSAndD, '4g' nettype, nvl(b.provid, '0') provid, nvl(b.provname, '0') provname,
         |            nvl(b.cityid, '0') cityid, nvl(b.cityname, '0') cityname,
         |            h.enbid, nvl(b.zhlabel,'0') enbname,
         |            h.a_sn, h.a_rn, h.a_fn, h.a_rcn, h.a_fcn
         |    from    ${tmpCurHtable} h left join ${bs4gTable} b on(h.enbid = b.enbid)
         |    where  h.nettype='4g'
         |) t
       """.stripMargin


    sqlContext.sql(bsSQL)

    // 由于3g和4g基站的编码不会重复， 所以可以采用3/4/t一块入hbase
    val bsNetStatSQL =
      s"""
         |select provname, cAndSAndD, enbid, enbname, nettype,
         |       a_sn, a_rn, a_fn, a_rcn, a_fcn,
         |       row_number() over(partition by cAndSAndD, nettype order by a_fn desc ) a_net_rank,
         |       row_number() over(partition by cAndSAndD, nettype order by a_fcn desc ) a_card_rank
         |from ${bsTable} t
         """.stripMargin
    val bsNetResult = sqlContext.sql(bsNetStatSQL)

    val bsNetStatRDD = bsNetResult.coalesce(10).rdd.map(x => {
      val aNetRank = if (null == x(10)) "0" else String.format("%3s", x(10).toString).replaceAll(" ", "0")
      val aCardRank = if (null == x(11)) "0" else String.format("%3s", x(11).toString).replaceAll(" ", "0")

      val proName = if (null == x(0)) "其他" else x(0).toString
      val csd = if (null == x(1)) "0" else x(1).toString
      val aNetRow = csd + "_" + aNetRank
      val aCardRow = csd + "_" + aCardRank

      val bid = if (null == x(2)) "0" else x(2).toString
      val bname = if (null == x(3)) "0" else x(3).toString
      val net = if (null == x(4)) "0" else x(4).toString
      val netFlag = "t"


      val asn = if (null == x(5)) "0" else x(5).toString
      val arn = if (null == x(6)) "0" else x(6).toString
      val afn = if (null == x(7)) "0" else x(7).toString
      val arcn = if (null == x(8)) "0" else x(8).toString
      val afcn = if (null == x(9)) "0" else x(9).toString


      val aNetPut = if (aNetRank.toDouble < 1000) new Put(Bytes.toBytes(aNetRow)) else null
      val aCardPut = if (aCardRank.toDouble < 1000) new Put(Bytes.toBytes(aCardRow)) else null


      var authNetTuple: Tuple2[ImmutableBytesWritable, Put] = null
      var authCardTuple: Tuple2[ImmutableBytesWritable, Put] = null


      if (aNetPut != null && afn >= "1") {
        aNetPut.addColumn(Bytes.toBytes("r"), Bytes.toBytes("a_" + analyType + "_t_rn_bd"), Bytes.toBytes(bid))
        aNetPut.addColumn(Bytes.toBytes("r"), Bytes.toBytes("a_" + analyType + "_t_rn_bn"), Bytes.toBytes(bname))
        aNetPut.addColumn(Bytes.toBytes("r"), Bytes.toBytes("a_" + analyType + "_t_rn_bv"), Bytes.toBytes(arn))
        aNetPut.addColumn(Bytes.toBytes("r"), Bytes.toBytes("a_" + analyType + "_t_rfn_bd"), Bytes.toBytes(bid))
        aNetPut.addColumn(Bytes.toBytes("r"), Bytes.toBytes("a_" + analyType + "_t_rfn_bn"), Bytes.toBytes(bname))
        aNetPut.addColumn(Bytes.toBytes("r"), Bytes.toBytes("a_" + analyType + "_t_rfn_bv"), Bytes.toBytes(afn))
        authNetTuple = (new ImmutableBytesWritable, aNetPut)
      }

      if (aCardPut != null && afcn >= "1") {
        aCardPut.addColumn(Bytes.toBytes("r"), Bytes.toBytes("a_" + analyType + "_t_rcn_bd"), Bytes.toBytes(bid))
        aCardPut.addColumn(Bytes.toBytes("r"), Bytes.toBytes("a_" + analyType + "_t_rcn_bn"), Bytes.toBytes(bname))
        aCardPut.addColumn(Bytes.toBytes("r"), Bytes.toBytes("a_" + analyType + "_t_rcn_bv"), Bytes.toBytes(arcn))
        aCardPut.addColumn(Bytes.toBytes("r"), Bytes.toBytes("a_" + analyType + "_t_rfcn_bd"), Bytes.toBytes(bid))
        aCardPut.addColumn(Bytes.toBytes("r"), Bytes.toBytes("a_" + analyType + "_t_rfcn_bn"), Bytes.toBytes(bname))
        aCardPut.addColumn(Bytes.toBytes("r"), Bytes.toBytes("a_" + analyType + "_t_rfcn_bv"), Bytes.toBytes(afcn))
        authCardTuple = (new ImmutableBytesWritable, aCardPut)
      }


      (authNetTuple, authCardTuple)

    })


    // bsNetStatRDD.filter(null != _._1).map(_._1).map(x=>x.toString()).take(5)

    bsNetStatRDD.filter(null != _._2).map(_._2).map(x=>x.toString()).take(5)

    HbaseDataUtil.saveRddToHbase(areaRankHtable, bsNetStatRDD.filter(null != _._1).map(_._1))
    HbaseDataUtil.saveRddToHbase(areaRankHtable, bsNetStatRDD.filter(null != _._2).map(_._2))


    val cityStatSQL =
      s"""select cAndSAndD, cityname, nettype,
         |       a_sn, a_rn, a_fn,
         |       a_rcn, a_fcn,
         |       row_number() over(partition by cAndSAndD, nettype order by a_fn desc) a_net_rank,
         |       row_number() over(partition by cAndSAndD, nettype order by a_fcn desc) a_card_rank
         |from
         |(
         |    select cAndSAndD, nvl(nettype, 't') nettype, cityname,
         |           sum(a_sn) as a_sn, sum(a_rn) as a_rn, sum(a_fn) as a_fn, sum(a_rcn) as a_rcn,
         |           sum(a_fcn) as a_fcn
         |    from ${bsTable}
         |    group by cAndSAndD, nettype, cityname
         |    grouping sets((cAndSAndD, cityname), (cAndSAndD, nettype,  cityname))
         |) t
       """.stripMargin

    val cityResultDF = sqlContext.sql(cityStatSQL)
    val cityStatRDD = cityResultDF.coalesce(10).rdd.map(x => {


      val aNetRank = if (null == x(8)) "0" else String.format("%3s", x(8).toString).replaceAll(" ", "0")
      val aCardRank = if (null == x(9)) "0" else String.format("%3s", x(9).toString).replaceAll(" ", "0")

      val csd = if (null == x(0)) "0" else x(0).toString
      val aNetRow = csd + "_" + aNetRank
      val aCardRow = csd + "_" + aCardRank

      val cname = if (null == x(1)) "0" else x(1).toString
      val net = if (null == x(2)) "0" else x(2).toString
      val netFlag = if ("3g" == net) "3" else if ("4g" == net) "4" else net


      val asn = if (null == x(3)) "0" else x(3).toString
      val arn = if (null == x(4)) "0" else x(4).toString
      val afn = if (null == x(5)) "0" else x(5).toString
      val arcn = if (null == x(6)) "0" else x(6).toString
      val afcn = if (null == x(7)) "0" else x(7).toString


      val aNetPut = if (aNetRank.toDouble < 1000) new Put(Bytes.toBytes(aNetRow)) else null
      val aCardPut = if (aCardRank.toDouble < 1000) new Put(Bytes.toBytes(aCardRow)) else null


      var authNetTuple: Tuple2[ImmutableBytesWritable, Put] = null
      var authCardTuple: Tuple2[ImmutableBytesWritable, Put] = null


      if (aNetPut != null && afn >= "1") {

        aNetPut.addColumn(Bytes.toBytes("r"), Bytes.toBytes("a_" + analyType + "_" + netFlag + "_rn_rd"), Bytes.toBytes(cname))
        aNetPut.addColumn(Bytes.toBytes("r"), Bytes.toBytes("a_" + analyType + "_" + netFlag + "_rn_rn"), Bytes.toBytes(cname))
        aNetPut.addColumn(Bytes.toBytes("r"), Bytes.toBytes("a_" + analyType + "_" + netFlag + "_rn_rv"), Bytes.toBytes(arn))
        aNetPut.addColumn(Bytes.toBytes("r"), Bytes.toBytes("a_" + analyType + "_" + netFlag + "_rfn_rd"), Bytes.toBytes(cname))
        aNetPut.addColumn(Bytes.toBytes("r"), Bytes.toBytes("a_" + analyType + "_" + netFlag + "_rfn_rn"), Bytes.toBytes(cname))
        aNetPut.addColumn(Bytes.toBytes("r"), Bytes.toBytes("a_" + analyType + "_" + netFlag + "_rfn_rv"), Bytes.toBytes(afn))

        authNetTuple = (new ImmutableBytesWritable, aNetPut)
      }


      if (aCardPut != null && afcn >= "1") {

        aCardPut.addColumn(Bytes.toBytes("r"), Bytes.toBytes("a_" + analyType + "_" + netFlag + "_rcn_rd"), Bytes.toBytes(cname))
        aCardPut.addColumn(Bytes.toBytes("r"), Bytes.toBytes("a_" + analyType + "_" + netFlag + "_rcn_rn"), Bytes.toBytes(cname))
        aCardPut.addColumn(Bytes.toBytes("r"), Bytes.toBytes("a_" + analyType + "_" + netFlag + "_rcn_rv"), Bytes.toBytes(arcn))
        aCardPut.addColumn(Bytes.toBytes("r"), Bytes.toBytes("a_" + analyType + "_" + netFlag + "_rfcn_rd"), Bytes.toBytes(cname))
        aCardPut.addColumn(Bytes.toBytes("r"), Bytes.toBytes("a_" + analyType + "_" + netFlag + "_rfcn_rn"), Bytes.toBytes(cname))
        aCardPut.addColumn(Bytes.toBytes("r"), Bytes.toBytes("a_" + analyType + "_" + netFlag + "_rfcn_rv"), Bytes.toBytes(afcn))

        authCardTuple = (new ImmutableBytesWritable, aCardPut)
      }


      (authNetTuple, authCardTuple)

    })

    HbaseDataUtil.saveRddToHbase(areaRankHtable, cityStatRDD.filter(null != _._1).map(_._1))
    HbaseDataUtil.saveRddToHbase(areaRankHtable, cityStatRDD.filter(null != _._2).map(_._2))


    val proStatSQL =
      s"""
         |select cAndSAndD, provname, nettype,
         |       a_sn, a_rn, a_fn,
         |       a_rcn, a_fcn,
         |       row_number() over(partition by cAndSAndD, nettype order by a_fn desc) a_net_rank,
         |       row_number() over(partition by cAndSAndD, nettype order by a_fcn desc) a_card_rank
         |from
         |(
         |    select cAndSAndD, nvl(nettype, 't') nettype, provname,
         |           sum(a_sn) as a_sn, sum(a_rn) as a_rn, sum(a_fn) as a_fn, sum(a_rcn) as a_rcn,
         |           sum(a_fcn) as a_fcn
         |    from ${bsTable}
         |    group by cAndSAndD, nettype, provname
         |    grouping sets((cAndSAndD, provname), (cAndSAndD, nettype,  provname))
         |) t
       """.stripMargin

    val proResultDF = sqlContext.sql(proStatSQL)
    val proStatRDD = proResultDF.coalesce(10).rdd.map(x => {


      val aNetRank = if (null == x(8)) "0" else String.format("%3s", x(8).toString).replaceAll(" ", "0")
      val aCardRank = if (null == x(9)) "0" else String.format("%3s", x(9).toString).replaceAll(" ", "0")

      val csd = if (null == x(0)) "0" else x(0).toString
      val aNetRow = csd + "_" + aNetRank
      val aCardRow = csd + "_" + aCardRank

      val pname = if (null == x(1)) "0" else x(1).toString
      val net = if (null == x(2)) "0" else x(2).toString
      val netFlag = if ("3g" == net) "3" else if ("4g" == net) "4" else net


      val asn = if (null == x(3)) "0" else x(3).toString
      val arn = if (null == x(4)) "0" else x(4).toString
      val afn = if (null == x(5)) "0" else x(5).toString
      val arcn = if (null == x(6)) "0" else x(6).toString
      val afcn = if (null == x(7)) "0" else x(7).toString


      val aNetPut = if (aNetRank.toDouble < 1000) new Put(Bytes.toBytes(aNetRow)) else null
      val aCardPut = if (aCardRank.toDouble < 1000) new Put(Bytes.toBytes(aCardRow)) else null


      var authNetTuple: Tuple2[ImmutableBytesWritable, Put] = null
      var authCardTuple: Tuple2[ImmutableBytesWritable, Put] = null


      if (aNetPut != null && afn >= "1") {

        aNetPut.addColumn(Bytes.toBytes("r"), Bytes.toBytes("a_" + analyType + "_" + netFlag + "_rn_pd"), Bytes.toBytes(pname))
        aNetPut.addColumn(Bytes.toBytes("r"), Bytes.toBytes("a_" + analyType + "_" + netFlag + "_rn_pn"), Bytes.toBytes(pname))
        aNetPut.addColumn(Bytes.toBytes("r"), Bytes.toBytes("a_" + analyType + "_" + netFlag + "_rn_pv"), Bytes.toBytes(arn))
        aNetPut.addColumn(Bytes.toBytes("r"), Bytes.toBytes("a_" + analyType + "_" + netFlag + "_rfn_pd"), Bytes.toBytes(pname))
        aNetPut.addColumn(Bytes.toBytes("r"), Bytes.toBytes("a_" + analyType + "_" + netFlag + "_rfn_pn"), Bytes.toBytes(pname))
        aNetPut.addColumn(Bytes.toBytes("r"), Bytes.toBytes("a_" + analyType + "_" + netFlag + "_rfn_pv"), Bytes.toBytes(afn))

        authNetTuple = (new ImmutableBytesWritable, aNetPut)
      }


      if (aCardPut != null && afcn >= "1") {

        aCardPut.addColumn(Bytes.toBytes("r"), Bytes.toBytes("a_" + analyType + "_" + netFlag + "_rcn_pd"), Bytes.toBytes(pname))
        aCardPut.addColumn(Bytes.toBytes("r"), Bytes.toBytes("a_" + analyType + "_" + netFlag + "_rcn_pn"), Bytes.toBytes(pname))
        aCardPut.addColumn(Bytes.toBytes("r"), Bytes.toBytes("a_" + analyType + "_" + netFlag + "_rcn_pv"), Bytes.toBytes(arcn))
        aCardPut.addColumn(Bytes.toBytes("r"), Bytes.toBytes("a_" + analyType + "_" + netFlag + "_rfcn_pd"), Bytes.toBytes(pname))
        aCardPut.addColumn(Bytes.toBytes("r"), Bytes.toBytes("a_" + analyType + "_" + netFlag + "_rfcn_pn"), Bytes.toBytes(pname))
        aCardPut.addColumn(Bytes.toBytes("r"), Bytes.toBytes("a_" + analyType + "_" + netFlag + "_rfcn_pv"), Bytes.toBytes(afcn))

        authCardTuple = (new ImmutableBytesWritable, aCardPut)
      }


      (authNetTuple, authCardTuple)

    })


    HbaseDataUtil.saveRddToHbase(areaRankHtable, proStatRDD.filter(null != _._1).map(_._1))
    HbaseDataUtil.saveRddToHbase(areaRankHtable, proStatRDD.filter(null != _._2).map(_._2))


    // 更新时间, 断点时间比数据时间多1分钟
    val updateTime = DateUtils.timeCalcWithFormatConvertSafe(dataTime, "yyyyMMddHHmm", 1 * 60, "yyyyMMddHHmm")
    //HbaseUtils.upSertColumnByRowkey(analyzeBPHtable, "bp", "rank_byArea_bptime", "rank_byArea_bptime", updateTime)

  }
}
