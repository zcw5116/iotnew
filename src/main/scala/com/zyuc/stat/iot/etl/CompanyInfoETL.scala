package com.zyuc.stat.iot.etl

import com.zyuc.stat.iot.etl.util.CompanyConverterUtils
import com.zyuc.stat.properties.ConfigProperties
import com.zyuc.stat.utils.CharacterEncodeConversion
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{Logging, SparkConf, SparkContext}

/**
  * Created by zhoucw on 17-8-18.
  */
object CompanyInfoETL extends Logging{
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()//.setAppName("OperalogAnalysis").setMaster("local[4]")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    sqlContext.sql("use " + ConfigProperties.IOT_HIVE_DATABASE)


    val inputPath = sc.getConf.get("spark.app.inputPath")
    val outputPath = sc.getConf.get("spark.app.outputPath")
    val companyTable = sc.getConf.get("spark.app.companyTable")
    val loadTime = sc.getConf.get("spark.app.loadTime")

    val srcRDD =CharacterEncodeConversion.transfer(sc, inputPath, "GBK")

    val CompanyDF = sqlContext.createDataFrame(srcRDD.map(x=>CompanyConverterUtils.parseLine(x)).filter(_.length != 1), CompanyConverterUtils.struct)

    val tmpLocations = outputPath + "/temp"
    val dataLocations = outputPath + "/data"

    CompanyDF.write.mode(SaveMode.Overwrite).format("orc").save(tmpLocations)
    val fileSystem = FileSystem.newInstance(sc.hadoopConfiguration)

    val tmpPath = new Path(tmpLocations + "/*.orc")
    val dataPath = new Path(dataLocations + "/*.orc")

    fileSystem.globStatus(dataPath).foreach(x=> fileSystem.delete(x.getPath(),false))

    val tmpStatus = fileSystem.globStatus(tmpPath)

    var num = 0
    tmpStatus.map(tmpStat => {
      val tmpLocation = tmpStat.getPath().toString
      var dataLocation = tmpLocation.replace(outputPath + "temp/", outputPath + "data/")
      val index = dataLocation.lastIndexOf("/")
      dataLocation = dataLocation.substring(0, index + 1) + loadTime + "-" + num + ".orc"
      num = num + 1

      val tmpPath = new Path(tmpLocation)
      val dataPath = new Path(dataLocation)

      if (!fileSystem.exists(dataPath.getParent)) {
        fileSystem.mkdirs(dataPath.getParent)
      }
      fileSystem.rename(tmpPath, dataPath)
    })

  }
}
