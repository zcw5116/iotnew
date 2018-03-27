package com.zyuc.stat.iot.dao

import com.zyuc.stat.properties.ConfigProperties
import com.zyuc.stat.utils.HbaseUtils
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.Logging
import org.apache.spark.sql.DataFrame

/**
  * Created by zhoucw on 17-8-8.
  */
object CDRDataDAO extends Logging{

  def saveRddData(df:DataFrame, hbaseTable:String) = {


  }
}
