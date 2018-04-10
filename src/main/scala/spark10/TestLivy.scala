package spark10

import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zhoucw on 上午3:56.
  */
object TestLivy {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()//.setMaster("local[*]").setAppName("test")
    val sc = new SparkContext(sparkConf)

    val sqlContext = new SQLContext(sc)

    val list = List("1", "2", "a", "b", "c", "d", "e", "f", "g", "h")

    val rdd = sc.makeRDD(list)

    import sqlContext.implicits._
    val df = rdd.toDF("name")
    df.write.format("text").mode(SaveMode.Overwrite).save("/tmp/mylivy")
  }
}
