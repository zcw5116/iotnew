package com.zyuc.test

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by zhoucw on 18-5-11 下午11:30.
  */
object GeneData {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("NbM5Analysis_201805101400")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)

    import sqlContext.implicits._
    val dataDF = sc.textFile("/tmp/nbpgwtest.txt").map(x=>x.split("\\t")).
      map(x=>(x(0), x(2), x(3), x(4), x(5))).toDF("mdn", "prov", "city",
      "l_datavolumefbcuplink", "l_datavolumefbcdownlink")

    dataDF.write.format("orc").save("/user/epciot/data/cdr/transform/nb/data/d=180510/h=14/m5=00")
  }
}
