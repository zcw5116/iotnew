package com.zyuc.stat.iot.analysis.baseline

import com.zyuc.stat.iot.analysis.util.{OnlineHtableConverter, HbaseDataUtil}
import com.zyuc.stat.properties.ConfigProperties
import com.zyuc.stat.utils.{DateUtils, HbaseUtils}
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}


/**
  * desc: 计算基线， 计算规则： 取前N天的数据， 去掉最大值和最小值后取均值， 如果N不大于2， 那么无需去掉最大值和最小值
  * @author zhoucw
  * @version 1.0
  */
object OnlineBaseLine {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()//.setMaster("local[4]").setAppName("fd")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    sqlContext.sql("use " + ConfigProperties.IOT_HIVE_DATABASE)

    val appName = sc.getConf.get("spark.app.name") // name_2017073111
    val endDayid = sc.getConf.get("spark.app.baseLine.endDayid","20171012") // "20170906"
    val dayM5Time = sc.getConf.get("spark.app.baseLine.dayM5Time","1900")
    val intervalDayNums = sc.getConf.get("spark.app.baseLine.intervalDayNums","1").toInt //
    val modeName = sc.getConf.get("spark.app.baseLine.modeName", "online") // auth mme flow online
    val alarmHtablePre = sc.getConf.get("spark.app.htable.alarmTablePre", "analyze_summ_tab_")
    val resultHtablePre = sc.getConf.get("spark.app.htable.resultHtablePre", "analyze_summ_rst_")
    val targetdayid = sc.getConf.get("spark.app.htable.targetdayid", "20171012")
    // 实时分析类型： 0-后续会离线重跑数据, 2-后续不会离线重跑数据
    // val progRunType = sc.getConf.get("spark.app.progRunType", "0")


    /////////////////////////////////////////////////////////////////////////////////////////
    //  Hbase 相关的表
    //  表不存在， 就创建
    /////////////////////////////////////////////////////////////////////////////////////////
    //  alarmHtable-预警表
    val alarmHtable = alarmHtablePre + modeName + "_" + targetdayid
    val alarmFamilies = new Array[String](2)
    alarmFamilies(0) = "s"
    alarmFamilies(1) = "e"
    // 创建表, 如果表存在， 自动忽略
    HbaseUtils.createIfNotExists(alarmHtable,alarmFamilies)

    //  resultHtable-结果表,
    val resultHtable = resultHtablePre + modeName + "_" + targetdayid
    val resultFamilies = new Array[String](2)
    resultFamilies(0) = "s"
    resultFamilies(1) = "e"
    HbaseUtils.createIfNotExists(resultHtable, resultFamilies)



    // 将每天的Hbase数据union all后映射为一个DataFrame
    var hbaseDF:DataFrame = null
    for(i <- 0 until intervalDayNums + 1){
      val dayid = DateUtils.timeCalcWithFormatConvertSafe(endDayid, "yyyyMMdd", -i*24*60*60, "yyyyMMdd")
      if(i == 0){
        println("1")
        hbaseDF = OnlineHtableConverter.convertToDF(sc, sqlContext, resultHtablePre + modeName + "_" + dayid, null).filter(s"time<'${dayM5Time}'")
      }
      if(i== intervalDayNums){
        println("2")
        hbaseDF = hbaseDF.unionAll(OnlineHtableConverter.convertToDF(sc, sqlContext, resultHtablePre + modeName + "_" + dayid, null).filter(s"time>='${dayM5Time}'"))
      }
      else if(i>0){
        hbaseDF = hbaseDF.unionAll(OnlineHtableConverter.convertToDF(sc, sqlContext, resultHtablePre + modeName + "_" + dayid, null))
      }
    }


    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // 对hbase的dataframe 做处理
    // a_c_3_rat： 认证3g成功率           a_c_4_rat： 认证4g成功率
    // a_c_v_rat： 认证vpdn成功率         a_c_t_rat： 总的认证成功率
    // a_c_3_crat: 认证3g卡数到成功率      a_c_4_crat： 认证4g卡数的成功率
    // a_c_v_crat： 认证vpdn卡数到成功率   a_c_t_crat： 认证总的卡数成功率
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    hbaseDF = hbaseDF.selectExpr("compnyAndSerAndDomain", "time", "o_c_3_on", "o_c_4_on", "o_c_t_on")
    val tmpTable = "tmpHbase_" + endDayid
    hbaseDF.registerTempTable(tmpTable)


    //  对每个5分钟点的数据做分析函数排序
    val tmpSql =
      s"""
         |select compnyAndSerAndDomain, time,
         |o_c_3_on, o_c_4_on, o_c_t_on,
         |row_number() over(partition by compnyAndSerAndDomain, time order by cast(o_c_3_on as double)) o_c_3_on_rn,
         |row_number() over(partition by compnyAndSerAndDomain, time order by cast(o_c_4_on as double)) o_c_4_on_rn,
         |row_number() over(partition by compnyAndSerAndDomain, time order by cast(o_c_t_on as double)) o_c_t_on_rn
         |from ${tmpTable}
       """.stripMargin

    val tmpBaselineTable = "tmpBaselineTable"
    val tmpDF = sqlContext.sql(tmpSql)
    tmpDF.registerTempTable(tmpBaselineTable)

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    //  去掉最大值和最小值， 求均值
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    var aggSQL =
      s"""
         |select compnyAndSerAndDomain, time,
         |sum(case when o_c_3_on_rn>1 and o_c_3_on_rn< ${intervalDayNums} then o_c_3_on else 0 end)/(${intervalDayNums} - 2) as o_b_3_on,
         |sum(case when o_c_4_on_rn>1 and o_c_4_on_rn< ${intervalDayNums} then o_c_4_on else 0 end)/(${intervalDayNums} - 2) as o_b_4_on,
         |sum(case when o_c_t_on_rn>1 and o_c_t_on_rn< ${intervalDayNums} then o_c_t_on else 0 end)/(${intervalDayNums} - 2) as o_b_t_on
         |from ${tmpBaselineTable}
         |group by compnyAndSerAndDomain, time
       """.stripMargin

    // 如果天数N不大于2， 那么无需去掉最大值和最小值
    if(intervalDayNums <= 2){
      aggSQL =
        s"""
           |select compnyAndSerAndDomain, time,
           |sum(o_c_3_on)/${intervalDayNums} as o_b_3_on,
           |sum(o_c_4_on)/${intervalDayNums} as o_b_4_on,
           |sum(o_c_t_on)/${intervalDayNums} as o_b_t_on
           |from ${tmpBaselineTable}
           |group by compnyAndSerAndDomain, time
       """.stripMargin
    }


  val resultDF = sqlContext.sql(aggSQL).coalesce(1)


    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    //  将数据写入hbase表
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    val resultRDD = resultDF.rdd.map(x=>{
      val companyAndDomain = x(0).toString
      val time = x(1).toString
      val o_b_3_on = x(2).toString
      val o_b_4_on =  x(3).toString
      val o_b_t_on = x(4).toString

      val alramkey0 = "0_" + time + "_" + companyAndDomain
      val alramkey1 = "1_" + time + "_" + companyAndDomain
      val resultkey = companyAndDomain + "_" + time

      val putAlarm0 = new Put(Bytes.toBytes(alramkey0))
      val putAlarm1 = new Put(Bytes.toBytes(alramkey1))
      val resultPut = new Put(Bytes.toBytes(resultkey))

      putAlarm0.addColumn(Bytes.toBytes("s"), Bytes.toBytes("o_b_3_on"), Bytes.toBytes(o_b_3_on))
      putAlarm0.addColumn(Bytes.toBytes("s"), Bytes.toBytes("o_b_4_on"), Bytes.toBytes(o_b_4_on))
      putAlarm0.addColumn(Bytes.toBytes("s"), Bytes.toBytes("o_b_t_on"), Bytes.toBytes(o_b_t_on))

      putAlarm1.addColumn(Bytes.toBytes("s"), Bytes.toBytes("o_b_3_on"), Bytes.toBytes(o_b_3_on))
      putAlarm1.addColumn(Bytes.toBytes("s"), Bytes.toBytes("o_b_4_on"), Bytes.toBytes(o_b_4_on))
      putAlarm1.addColumn(Bytes.toBytes("s"), Bytes.toBytes("o_b_t_on"), Bytes.toBytes(o_b_t_on))

      resultPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("o_b_3_on"), Bytes.toBytes(o_b_3_on))
      resultPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("o_b_4_on"), Bytes.toBytes(o_b_4_on))
      resultPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("o_b_t_on"), Bytes.toBytes(o_b_t_on))

      ((new ImmutableBytesWritable, putAlarm0), (new ImmutableBytesWritable, putAlarm1), (new ImmutableBytesWritable, resultPut))
    })


    HbaseDataUtil.saveRddToHbase(alarmHtable, resultRDD.map(x=>x._1))
    HbaseDataUtil.saveRddToHbase(alarmHtable, resultRDD.map(x=>x._2))
    HbaseDataUtil.saveRddToHbase(resultHtable, resultRDD.map(x=>x._3))


  }

}
