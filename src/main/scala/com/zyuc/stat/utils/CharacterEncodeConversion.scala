package com.zyuc.stat.utils

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Created by slview on 17-6-17.
  * 编码转换，解决读取gbk编码中文乱码问题
  */
object CharacterEncodeConversion {
  //通过封装后的方法读取fileencode编码的文件, 并且每一行数据以字符串格式返回(RDD[String])
  def transfer(sc:SparkContext, path:String, fileencode:String):RDD[String]={
    sc.hadoopFile(path,classOf[TextInputFormat],classOf[LongWritable],classOf[Text],1)
      .map(p => new String(p._2.getBytes, 0, p._2.getLength, fileencode))
  }
}
