package test

package main.scala.com.cqfrog.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds,StreamingContext}
import org.apache.spark.streaming.StreamingContext._

/**
  * Created by grid on 9/29/16.
  */
object WordCount {
  def main (args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("TestWordCount").setMaster("local[2]")

    val ssc = new StreamingContext(sparkConf, Seconds(10))

    val lines = ssc.textFileStream("/hadoop/word/")
    val words = lines.flatMap(_.split(" "))

    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()

  }

}