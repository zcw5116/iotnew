package com.zyuc.stat.iot.analysis.realtime

import com.zyuc.stat.iot.analysis.util.{AreaHtableConverter, HbaseDataUtil}
import com.zyuc.stat.iot.user.OnlineBase.{logError, logInfo}
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
object AreaBaseAnalysis extends Logging {

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
    val appName = sc.getConf.get("spark.app.name", "name_201710272355") // name_201708010040

    val resultBSHtable = sc.getConf.get("spark.app.htable.resultBSHtable", "analyze_summ_rst_bs")
    val resultBSBaseLineHtable = sc.getConf.get("spark.app.htable.resultBSBaseLineHtable", "analyze_summ_rst_bs_baseline")
    val areaRankHtable = sc.getConf.get("spark.app.htable.areaRankHtable", "analyze_summ_rst_area_rank")
    val bs4gTable = sc.getConf.get("spark.app.table.bs4gTable", "iot_basestation_4g")
    val bs3gTable = sc.getConf.get("spark.app.table.bs4gTable", "iot_basestation_3g")

    val analyzeBPHtable = sc.getConf.get("spark.app.htable.analyzeBPHtable", "analyze_bp_tab")
    val ifUpdateBaseDataTime = sc.getConf.get("spark.app.ifUpdateBaseDataTime", "Y")

    if(ifUpdateBaseDataTime != "Y" && ifUpdateBaseDataTime != "N" ){
      logError("ifUpdateBaseDataTime类型错误错误, 期望值：Y, N ")
      return
    }

    // resultBSHtable
    val bsFamilies = new Array[String](1)
    bsFamilies(0) = "r"
    HbaseUtils.createIfNotExists(resultBSBaseLineHtable, bsFamilies)



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
    val dataDayid = dataTime.substring(0, 8)
    val rowStart = dataDayid + "0000"
    val rowEnd = dataTime + "_" +"5"  // 201710261310_4g_P100002368_C_-1_3608FFFF1C70
    val hbaseDF = AreaHtableConverter.convertToDF(sc, sqlContext, resultBSHtable, (rowStart, rowEnd))
    val tmpHtable = "tmpHtable_" + dataTime
    hbaseDF.registerTempTable(tmpHtable)
    val bsSQL =
      s"""
         |select h.cAndSAndD, '3g' nettype,
         |       nvl(b.provcode, '0') provid, nvl(b.provname, '0') provname,
         |       nvl(b.citycode, '0') cityid, nvl(b.cityname, '0') cityname,
         |       nvl(h.enbid, '0') enbid,
         |       nvl(concat_ws('_', h.enbid, b.cityname), nvl(h.enbid, '0'))  enbname,
         |       h.ma_sn, h.ma_rn,
         |       h.a_sn, h.a_rn,
         |       h.f_d, h.f_u,
         |       h.o_ln, h.o_fln
         |from ${tmpHtable} h left join ${bs3gTable} b on(substr(h.enbid, 1, 4) = b.bsidpre)
         |where h.nettype='3g'
         |union all
         |select h.cAndSAndD, '4g' nettype,
         |       nvl(b.provid, '0') provid, nvl(b.provname, '0') provname,
         |       nvl(b.cityid, '0') cityid, nvl(b.cityname, '0') cityname,
         |       nvl(h.enbid, '0') enbid,
         |       nvl(b.zhlabel,nvl(h.enbid, '0')) enbname,
         |       h.ma_sn, h.ma_rn,
         |       h.a_sn, h.a_rn,
         |       h.f_d, h.f_u,
         |       h.o_ln, h.o_fln
         |from   ${tmpHtable} h left join ${bs4gTable} b on(h.enbid = b.enbid)
         |where  h.nettype='4g'
       """.stripMargin
    val bsTable = "bsTable_" + dataTime
    sqlContext.sql(bsSQL).registerTempTable(bsTable)

    val bsNetTmpTable = "bsNetTmpTable_" + dataTime
    val bsnetSql =
      s"""    cache table ${bsNetTmpTable}  as
         |    select cAndSAndD, provid, provname, cityid, cityname, enbid, enbname, nettype,
         |           sum(ma_sn) ma_sn, sum(ma_rn) ma_rn,
         |           sum(a_sn) a_sn, sum(a_rn) a_rn,
         |           sum(f_d) f_d, sum(f_u) f_u, (sum(f_d) + sum(f_u)) f_t,
         |           sum(o_ln) o_ln, sum(o_fln) o_fln
         |    from ${bsTable}
         |    group by cAndSAndD, provid, provname, cityid, cityname, enbid, enbname, nettype
         """.stripMargin


    sqlContext.sql(bsnetSql)

    val resultDF = sqlContext.sql(
      s"""
         |select cAndSAndD, provid, provname, cityid, cityname, enbid, enbname, nettype, ma_sn, ma_rn, a_sn, a_rn, f_d, f_u, f_t, o_ln, o_fln
         |from ${bsNetTmpTable}
       """.stripMargin)

    val coalesceNums = 50
    val resultRDD = resultDF.coalesce(coalesceNums).rdd.map(x=>{
          val cAndSAndD = x(0).toString
          val provid = x(1).toString
          val provname = x(2).toString
          val cityid = x(3).toString
          val cityname = x(4).toString
          val enbid = x(5).toString
          val enbname = x(6).toString
          val nettype = x(7).toString
          val ma_sn = x(8).toString
          val ma_rn = x(9).toString
          val a_sn = x(10).toString
          val a_rn = x(11).toString
          val f_d = x(12).toString
          val f_u = x(13).toString
          val f_t = x(14).toString
          val o_ln = x(15).toString
          val o_fln = x(16).toString

          val rowkey = dataTime + "_" + nettype + "_" + cAndSAndD + "_" + enbid
          val put = new Put(Bytes.toBytes(rowkey))
          put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("cityid"), Bytes.toBytes(cityid))
          put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("cityname"), Bytes.toBytes(cityname))
          put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("provid"), Bytes.toBytes(provid))
          put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("provname"), Bytes.toBytes(provname))
          put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("enbname"), Bytes.toBytes(enbname))
          put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("ma_sn"), Bytes.toBytes(ma_sn))
          put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("ma_rn"), Bytes.toBytes(ma_rn))
          put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("a_sn"), Bytes.toBytes(a_sn))
          put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("a_rn"), Bytes.toBytes(a_rn))
          put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("f_d"), Bytes.toBytes(f_d))
          put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("f_u"), Bytes.toBytes(f_u))
          put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("f_t"), Bytes.toBytes(f_t))
          put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("o_ln"), Bytes.toBytes(o_ln))
          put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("o_fln"), Bytes.toBytes(o_fln))
          (new ImmutableBytesWritable, put)
    })

    HbaseDataUtil.saveRddToHbase(resultBSBaseLineHtable, resultRDD)

    // 写入hbase表
    if(ifUpdateBaseDataTime == "Y"){
      logInfo("write ifUpdateBaseDataTime to Hbase Table. ")
      HbaseUtils.updateCloumnValueByRowkey("iot_dynamic_data","area_basetime","basic","area_base_datatime", dataTime) // 从habase里面获取
    }

    }

}
