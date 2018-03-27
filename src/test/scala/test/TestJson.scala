package test

import com.zyuc.stat.properties.ConfigProperties
import org.apache.spark.sql.SaveMode
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.functions._

import scala.collection.mutable

/**
  * Created by zhoucw on 17-8-13.
  */
object TestJson {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[4]").setAppName("fd")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)

    val list = List(("K1", "10, 20, 30, 40,50", "a1,a2,a3,a4,a5"), ("K2", "10, 20, 30, 40, 50", "b1,b2,b3,b4,b5"))
    val yourDF = sc.parallelize(list)
    val newRDD = yourDF.flatMap(x => {
      val i = 10
      val key = x._1
      val nums = x._2.split(",")
      val ls = x._3.split(",")
      val r = nums.zip(ls)

      val set1 = new mutable.HashSet[Tuple15[String, String, String, String, String, String, String, String, String, String, String, String, String, String, String]]
      val set = new mutable.HashSet[Tuple3[String, String, String]]
      r.foreach(a => set.+=((key, a._1, a._2)))
      set
    }
    )

    newRDD.foreach(println)

  }

}
