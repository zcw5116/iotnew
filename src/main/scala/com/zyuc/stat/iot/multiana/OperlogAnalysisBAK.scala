package com.zyuc.stat.iot.multiana

import com.zyuc.stat.iot.etl.util.OperaLogConverterUtils
import com.zyuc.stat.iot.multiana.OperlogAnalysisBAK.operaSrcDF
import com.zyuc.stat.properties.ConfigProperties
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by zhoucw on 17-8-14.
  */
object OperlogAnalysisBAK {

  val sparkConf = new SparkConf().setAppName("OperalogAnalysis")//.setMaster("local[4]")
  val sc = new SparkContext(sparkConf)
  val sqlContext = new HiveContext(sc)
  sqlContext.sql("use " + ConfigProperties.IOT_HIVE_DATABASE)
  val operaDay = sc.getConf.get("spark.app.operaDay")

  val operaPath = "/hadoop/IOT/ANALY_PLATFORM/OperaLog/OutPut/data/"
  val operaTable = "operaTable_" + operaDay
  val operaSrcDF = sqlContext.read.format("orc").load(operaPath)
  val operaDF = operaSrcDF.filter(operaSrcDF.col("d") === operaDay &&
    operaSrcDF.col("logtype").isin(OperaLogConverterUtils.Oper_HLR_PLATFORM,OperaLogConverterUtils.Oper_HSS_PLATFORM) &&
    operaSrcDF.col("opername").isin(OperaLogConverterUtils.Oper_OPEN, OperaLogConverterUtils.Oper_CLOSE) &&
    operaSrcDF.col("oper_result") === "成功").select(operaSrcDF.col("mdn"), operaSrcDF.col("logtype"),
    operaSrcDF.col("vpdncompanycode"), operaSrcDF.col("custprovince"), operaSrcDF.col("opername")).registerTempTable(operaTable)

  val oper3GSQL =
    s"""select t.custprovince, t.vpdncompanycode, '2/3g' as nettype , count(*) as cnt from
       |(
       |    select t1.custprovince, t1.vpdncompanycode, t1.mdn, t2.mdn as mdn2
       |    from ${operaTable} t1 left join ${operaTable} t2
       |    on(t1.mdn = t2.mdn and t1.logtype='${OperaLogConverterUtils.Oper_HLR_PLATFORM}' and t2.logtype='${OperaLogConverterUtils.Oper_HSS_PLATFORM}')
       |) t
       |where t.mdn2 is null
       |group by t.custprovince, t.vpdncompanycode
     """.stripMargin

  val oper4GSQL =
    s"""select t.custprovince, t.vpdncompanycode, '4g' as nettype, count(*) as cnt from
       |(
       |    select t1.custprovince, t1.vpdncompanycode, t1.mdn, t2.mdn as mdn2
       |    from ${operaTable} t1 left join ${operaTable} t2
       |    on(t1.mdn = t2.mdn and t1.logtype='${OperaLogConverterUtils.Oper_HLR_PLATFORM}' and t2.logtype='${OperaLogConverterUtils.Oper_HSS_PLATFORM}')
       |) t
       |where t.mdn is null
       |group by t.custprovince, t.vpdncompanycode
     """.stripMargin

  val oper234GSQL =
    s"""select t.custprovince, t.vpdncompanycode, '2/3/4g' as nettype , count(*) as cnt from
       |(
       |    select t1.custprovince, t1.vpdncompanycode, t1.mdn, t2.mdn as mdn2
       |    from ${operaTable} t1 inner join ${operaTable} t2
       |    on(t1.mdn = t2.mdn and t1.logtype='${OperaLogConverterUtils.Oper_HLR_PLATFORM}' and t2.logtype='${OperaLogConverterUtils.Oper_HSS_PLATFORM}')
       |) t
       |group by t.custprovince, t.vpdncompanycode
     """.stripMargin

  val oper3GDF = sqlContext.sql(oper3GSQL)
  val oper4GDF = sqlContext.sql(oper4GSQL)
  val oper234GDF = sqlContext.sql(oper234GSQL)

  val resultDF = oper3GDF.unionAll(oper4GDF).unionAll(oper234GDF)

}
