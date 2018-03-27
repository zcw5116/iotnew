package com.zyuc.stat.iot.interphone

import com.zyuc.stat.properties.ConfigProperties
import org.apache.spark.sql.SaveMode
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.functions._
/**
  * Created by zhoucw on 17-8-4.
  */

case class SourceUser(id:Int, mdn:String)
object test {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    //.setMaster("local[4]").setAppName("fd")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    sqlContext.sql("use " + ConfigProperties.IOT_HIVE_DATABASE)

    import sqlContext.implicits._

    val sourceDF = sqlContext.read.format("text").load("/tmp/tmpuser.txt").map(x=>x.getString(0).split("\t")).map(x=>SourceUser(x(0).toInt,x(1))).toDF()

    val userDF = sqlContext.table("iot_customer_userinfo").filter("d=20170801" ).select("mdn","imsicdma", "vpdncompanycode")

    val joinDF = sourceDF.join(userDF,sourceDF.col("mdn")===userDF.col("mdn")).select(sourceDF.col("id"), sourceDF.col("mdn"), userDF.col("imsicdma"), userDF.col("vpdncompanycode"))
    joinDF.cache()


    //  3g
    val auth3GDF1 = sqlContext.table("iot_userauth_3gaaa").filter("dayid=20170727").filter("hourid>21").filter("hourid<24").select("auth_result", "auth_time", "imsicdma", "mdn", "dayid", "hourid")
    val auth3GDF2 = sqlContext.table("iot_userauth_3gaaa").filter("dayid=20170728").filter("hourid>0").filter("hourid<5").select("auth_result", "auth_time", "imsicdma", "mdn", "dayid", "hourid")
    val auth3GDF = auth3GDF1.unionAll(auth3GDF2)

    val auth3GResult = joinDF.join(auth3GDF, joinDF.col("imsicdma")===auth3GDF.col("imsicdma")).
      select(auth3GDF.col("auth_result"), auth3GDF.col("auth_time"), auth3GDF.col("imsicdma"), auth3GDF.col("dayid"), auth3GDF.col("hourid"), joinDF.col("mdn"))
    val auth3gLocation="/tmp/zcw/auth/3g"
    auth3GResult.write.format("orc").mode(SaveMode.Overwrite).save(auth3gLocation)
    sqlContext.sql(s"create external table tmp_auth3g (auth_result int, auth_time string, imsicdma string, dayid string, hourid string, mdn string) stored as orc location '$auth3gLocation'")
    sqlContext.sql("select dayid, hourid, auth_result, count(*) as cnt from tmp_auth3g group by  dayid, hourid, auth_result").collect().foreach(println)


    // 4g
    val auth4GDF1 = sqlContext.table("iot_userauth_4gaaa").filter("dayid=20170727").filter("hourid>21").filter("hourid<24").select("auth_result", "auth_time", "imsicdma", "mdn", "dayid", "hourid")
    val auth4GDF2 = sqlContext.table("iot_userauth_4gaaa").filter("dayid=20170728").filter("hourid>0").filter("hourid<5").select("auth_result", "auth_time", "imsicdma", "mdn", "dayid", "hourid")
    val auth4GDF = auth4GDF1.unionAll(auth4GDF2)
    val auth4GResult = joinDF.join(auth4GDF, joinDF.col("mdn")===auth4GDF.col("mdn")).
      select(auth4GDF.col("auth_result"), auth4GDF.col("auth_time"), auth4GDF.col("imsicdma"), auth4GDF.col("dayid"), auth4GDF.col("hourid"), joinDF.col("mdn"))

    val auth4gLocation="/tmp/zcw/auth/4g"
    auth4GResult.write.format("orc").mode(SaveMode.Overwrite).save(auth4gLocation)
    sqlContext.sql(s"create external table tmp_auth4g (auth_result int, auth_time string, imsicdma string, dayid string, hourid string, mdn string) stored as orc location '$auth4gLocation'")

    // vpdn
    val authVPDNDF1 = sqlContext.table("iot_userauth_vpdn").filter("dayid=20170727").filter("hourid>21").filter("hourid<24").select("auth_result", "auth_time", "imsicdma", "mdn", "dayid", "hourid")
    val authVPDNDF2 = sqlContext.table("iot_userauth_vpdn").filter("dayid=20170728").filter("hourid>0").filter("hourid<5").select("auth_result", "auth_time", "imsicdma", "mdn", "dayid", "hourid")
    val authVPDNDF = authVPDNDF1.unionAll(authVPDNDF2)
    val authVPDNResult = joinDF.join(authVPDNDF, joinDF.col("mdn")===authVPDNDF.col("mdn")).
      select(authVPDNDF.col("auth_result"), authVPDNDF.col("auth_time"), authVPDNDF.col("imsicdma"), authVPDNDF.col("dayid"), authVPDNDF.col("hourid"), joinDF.col("mdn"))

    val authvpdnLocation="/tmp/zcw/auth/vpdn"
    authVPDNResult.write.format("orc").mode(SaveMode.Overwrite).save(authvpdnLocation)
    sqlContext.sql(s"create external table tmp_authvpdn (auth_result int, auth_time string, imsicdma string, dayid string, hourid string, mdn string) stored as orc location '$authvpdnLocation'")

    // select mdn, time, dayid, status, terminatecause from pgwradius_out
    // 上下线
    val pgwRaidusDF1 = sqlContext.table("pgwradius_out").filter("dayid=20170727").filter("time>'20170727210000'").select("mdn", "time", "dayid", "status", "terminatecause")
    val pgwRaidusDF2 = sqlContext.table("pgwradius_out").filter("dayid=20170728").filter("time<'20170728050000'").select("mdn", "time", "dayid", "status", "terminatecause")
    val pgwRaidusDF = pgwRaidusDF1.unionAll(pgwRaidusDF2)

    val pgwDF = pgwRaidusDF.join(joinDF, joinDF.col("mdn")===pgwRaidusDF.col("mdn")).select(joinDF.col("mdn"), pgwRaidusDF.col("dayid"), pgwRaidusDF.col("time").substr(0,10).alias("hourid"),
      pgwRaidusDF.col("status"), pgwRaidusDF.col("terminatecause"))

    pgwDF.filter("length(terminatecause)>0").groupBy(pgwDF.col("hourid"),pgwDF.col("terminatecause")).agg(count(lit(1)).alias("tercase")).orderBy(pgwDF.col("hourid"))
    pgwDF.filter("terminatecause like '%code%'").select("hourid","mdn","terminatecause").show

    val a = sqlContext.table("pgwradius_out").filter("dayid='20170822'").filter("status='Start'").select("mdn","time")
    a.repartition(2).write.mode(SaveMode.Overwrite).save("/tmp/tmp")
    a.groupBy("mdn").count()
    // 流量
    // g3
/*    val g3DF1 = sqlContext.table("iot_cdr_3gaaa_ticket").filter("dayid=20170727").filter("hourid>'21'").select("event_time","mdn","originating","termination","dayid","hourid")
    val g3DF2 = sqlContext.table("iot_cdr_3gaaa_ticket").filter("dayid=20170728").filter("hourid<'03'").select("event_time","mdn","originating","termination","dayid","hourid")
    val g3DF = g3DF1.unionAll(g3DF2)

    val g3ResultDF = g3DF.join(joinDF, joinDF.col("mdn")===g3DF.col("mdn")).select(g3DF.col("mdn"), g3DF.col("dayid"), g3DF.col("hourid"), g3DF.col("originating"), g3DF.col("termination") )
    val g3R = g3ResultDF.groupBy(g3ResultDF.col("dayid"), g3ResultDF.col("hourid"), g3ResultDF.col("mdn")).agg(sum(col("originating")).alias("upflows"), sum(col("termination")).alias("downflows"))
    val g3flowLocation = "/tmp/zcw/flow/g3"
    g3R.write.format("orc").mode(SaveMode.Overwrite).save(g3flowLocation)
    sqlContext.sql(s"create external table tmp_flowg3_1 (dayid string, hourid string, mdn string,  upflows bigint, downflows bigint) stored as orc location '$g3flowLocation'")
        sqlContext.sql("select dayid, hourid, sum(upflows) as upflows, sum(downflows) as downflows from tmp_flowg3_1 group by dayid, hourid").collect().foreach(println)

    */
    //g4
    val g4DF1 = sqlContext.table("iot_cdr_pgw_ticket").filter("dayid=20170727").filter("hourid>'21'").select("l_timeoflastusage","mdn","l_datavolumefbcuplink","l_datavolumefbcdownlink","dayid","hourid")
    val g4DF2 = sqlContext.table("iot_cdr_pgw_ticket").filter("dayid=20170728").filter("hourid<'03'").select("l_timeoflastusage","mdn","l_datavolumefbcuplink","l_datavolumefbcdownlink","dayid","hourid")
    val g4DF = g4DF1.unionAll(g4DF2)

    val g4ResultDF = g4DF.join(joinDF, joinDF.col("mdn")===g4DF.col("mdn")).select(g4DF.col("mdn"), g4DF.col("dayid"), g4DF.col("hourid"), g4DF.col("l_datavolumefbcuplink"), g4DF.col("l_datavolumefbcdownlink") )
    val g4R = g4ResultDF.groupBy(g4ResultDF.col("dayid"), g4ResultDF.col("hourid"), g4ResultDF.col("mdn")).agg(sum(col("l_datavolumefbcuplink")).alias("upflows"), sum(col("l_datavolumefbcdownlink")).alias("downflows"))
    val g4flowLocation = "/tmp/zcw/flow/g4"
    g4R.write.format("orc").mode(SaveMode.Overwrite).save(g4flowLocation)
    sqlContext.sql(s"create external table tmp_flowg4 (dayid string, hourid string, mdn bigint,  upflows bigint, downflows bigint) stored as orc location '$g4flowLocation'")
    sqlContext.sql("select dayid, hourid, sum(upflows) as upflows, sum(downflows) as downflows from tmp_flowg4 group by dayid, hourid").collect().foreach(println)
  }

}
