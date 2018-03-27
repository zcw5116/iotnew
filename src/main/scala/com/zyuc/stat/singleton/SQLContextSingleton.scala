package com.zyuc.stat.singleton

import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext

/**
 * Created by slview on 17-6-13.
 * 通过单例模式获取sqlContext
 */
object SQLContextSingleton {
 @transient  private var instance: HiveContext = _
 def getInstance(sparkContext: SparkContext): HiveContext = {
   if (instance == null) {
     instance = new HiveContext(sparkContext)
   }
   instance
 }
}
