package com.zyuc.stat.iot.analysis.realtime

import com.zyuc.stat.iot.analysis.util.{AnalyRstCurHbaseToDF, HbaseDataUtil}
import com.zyuc.stat.properties.ConfigProperties
import com.zyuc.stat.utils.HbaseUtils
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{Logging, SparkConf, SparkContext}
import org.joda.time.DateTime
/**
  * Created by dell on 2017/10/10.
  * 企业地图
  * 只对公司及业务进行汇总
  * 1.analyze_summ_rst_everyday 取指定时间数据，对企业code进行汇总
  * 2.根据企业code关联iot_basic_company_and_domain表，获取省份信息provincename
  *
  * 3.更新断点时间
  * 4.维护一条ALL_ALL全国数据
  */

object CompanyInfoBaseRegionAnalysis extends Logging{
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    sqlContext.sql("use " + ConfigProperties.IOT_HIVE_DATABASE)

    //  获取参数
    val appName = sc.getConf.get("spark.app.name","name_201710010040") // name_201710010040
    val analyze_summ_rst_table= sc.getConf.get("spark.app.analyze_summ_rst_everyday","analyze_summ_rst_everyday") // analyze_summ_rst_everyday
    val companyAndDomainTable = sc.getConf.get("spark.app.table.companyAndDomainTable", "iot_basic_company_and_domain")
    val dayid = sc.getConf.get("spark.app.dayid")
    val HbaseRstTable = sc.getConf.get("spark.app.table.HbaseRstTable", "analyze_summ_rst_area")
    val analyzeBPHtable = sc.getConf.get("spark.app.table.analyzeBPHtable", "analyze_bp_tab")
    // 获取公司表信息
   // val companyAndDomainTable = "iot_basic_company_and_domain"
   // val analyze_summ_rst_table = "analyze_summ_rst_everyday"
    logInfo("##########--appName:                  " + appName)
    logInfo("##########--analyze_summ_rst_table:   " + analyze_summ_rst_table)
    logInfo("##########--companyAndDomainTable:    " + companyAndDomainTable)
    logInfo("##########--dayid:                    " + dayid)
    logInfo("##########--HbaseRstTable:            " + HbaseRstTable)
    logInfo("##########--analyzeBPHtable:          " + analyzeBPHtable)

    val summday = dayid.substring(2,8)
    val tmpCompanyTable = "tmp_company_info"
    val Analytablename = "tmp_analyze_summ_rst"
    val tmprstTable = "tmp_joinrst_Table"
    sqlContext.sql(
      s"""select c.provincecode, c.provincename, c.companycode, c.vpdndomain
         |from
         |(
         |select provincecode, provincename, companycode,
         |       (case when length(vpdndomain)=0 then null else vpdndomain end) as vpdndomain,
         |       row_number() over(partition by companycode) rn
         |from   ${companyAndDomainTable}
         |)c where c.rn=1
       """.stripMargin).registerTempTable(tmpCompanyTable)

    // 获取analyze_summ_rst_everyday数据,过滤为-1的项

    AnalyRstCurHbaseToDF.convertToDF(sc,sqlContext,analyze_summ_rst_table).
      filter(s"summtime = ${summday}").
      registerTempTable(Analytablename)
    // rowkey {YYMMDD}_{COMPANYCODE}_{SERVICETYPE}_{DOMAIN}
   val resultDF = sqlContext.sql(
     s"""
        | select c.provincecode,c.provincename,a.servtype,
        |        count(a.companycode) as c_d_t_cn,
        |        sum(a.c_d_t_un) as c_d_t_un,
        |        sum(a.o_c_t_on) as o_c_t_on,
        |        sum(a.o_d_t_on) as o_d_t_on,
        |        sum(a.f_c_3_t)  as f_c_3_t,
        |        sum(a.f_c_4_t)  as f_c_4_t,
        |        sum(a.f_c_t_t)  as f_c_t_t,
        |        sum(a.f_d_3_t)  as f_d_3_t,
        |        sum(a.f_d_4_t)  as f_d_4_t,
        |        sum(a.f_d_t_t)  as f_d_t_t,
        |        sum(a.ms_c_s_n) as ms_c_s_n,
        |        sum(a.ms_c_r_n) as ms_c_r_n,
        |        sum(a.ms_c_t_n) as ms_c_t_n,
        |        sum(a.ms_d_s_n) as ms_d_s_n,
        |        sum(a.ms_d_r_n) as ms_d_r_n,
        |        sum(a.ms_d_t_n) as ms_d_t_n
        | from   ${Analytablename} a
        | join   ${tmpCompanyTable} c on  a.companycode = c.companycode
        | group by provincecode,provincename,servtype
        | GROUPING SETS((provincecode,provincename,servtype),(servtype))
      """.stripMargin).coalesce(1)



    // 汇总结果写入Hbase中
    val resultRDD = resultDF.rdd.map(x=>{
      val provincecode = if(null == x(0)) "ALL" else  x(0).toString
      val provincename = if(null == x(1)) "ALL" else  x(1).toString
      val servtype     = x(2).toString
      val c_d_t_cn     = if(null == x(3)) "0"   else  x(3).toString
      val c_d_t_un     = if(null == x(4)) "0"   else  x(4).toString
      val o_c_t_on     = if(null == x(5)) "0"   else  x(5).toString
      val o_d_t_on     = if(null == x(6)) "0"   else  x(6).toString
      val f_c_3_t      = if(null == x(7)) "0"   else  x(7).toString
      val f_c_4_t      = if(null == x(8)) "0"   else  x(8).toString
      val f_c_t_t      = if(null == x(9)) "0"   else  x(9).toString
      val f_d_3_t      = if(null == x(10)) "0"  else  x(10).toString
      val f_d_4_t      = if(null == x(11)) "0"  else  x(11).toString
      val f_d_t_t      = if(null == x(12)) "0"  else  x(12).toString
      val ms_c_s_n     = if(null == x(13)) "0"  else  x(13).toString
      val ms_c_r_n     = if(null == x(14)) "0"  else  x(14).toString
      val ms_c_t_n     = if(null == x(15)) "0"  else  x(15).toString
      val ms_d_s_n     = if(null == x(16)) "0"  else  x(16).toString
      val ms_d_r_n     = if(null == x(17)) "0"  else  x(17).toString
      val ms_d_t_n     = if(null == x(18)) "0"  else  x(18).toString
      val rowKey =provincecode +"_"+ "ALL"+"_"+servtype
      val regionPut = new Put(Bytes.toBytes(rowKey))
      regionPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("areaname"), Bytes.toBytes(provincename))
      regionPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("c_d_t_cn"), Bytes.toBytes(c_d_t_cn))
      regionPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("c_d_t_un"), Bytes.toBytes(c_d_t_un))
      regionPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("o_c_t_on"), Bytes.toBytes(o_c_t_on))
      regionPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("o_d_t_on"), Bytes.toBytes(o_d_t_on))
      regionPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("f_c_3_t"), Bytes.toBytes(f_c_3_t))
      regionPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("f_c_4_t"), Bytes.toBytes(f_c_4_t))
      regionPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("f_c_t_t"), Bytes.toBytes(f_c_t_t))
      regionPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("f_d_3_t"), Bytes.toBytes(f_d_3_t))
      regionPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("f_d_4_t"), Bytes.toBytes(f_d_4_t))
      regionPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("f_d_t_t"), Bytes.toBytes(f_d_t_t))
      regionPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("ms_c_s_n"), Bytes.toBytes(ms_c_s_n))
      regionPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("ms_c_r_n"), Bytes.toBytes(ms_c_r_n))
      regionPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("ms_c_t_n"), Bytes.toBytes(ms_c_t_n))
      regionPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("ms_d_s_n"), Bytes.toBytes(ms_d_s_n))
      regionPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("ms_d_r_n"), Bytes.toBytes(ms_d_r_n))
      regionPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("ms_d_t_n"), Bytes.toBytes(ms_d_t_n))
      (new ImmutableBytesWritable, regionPut)
    })

    HbaseDataUtil.saveRddToHbase(HbaseRstTable, resultRDD)

    // 更新时间
    val updateTime = DateTime.now().toString("yyyyMMddHHmm")
    val analyzeColumn = "summ_byArea_bptime"
    HbaseUtils.upSertColumnByRowkey(analyzeBPHtable, "bp", "regionsumm", analyzeColumn, updateTime)
  }

}

