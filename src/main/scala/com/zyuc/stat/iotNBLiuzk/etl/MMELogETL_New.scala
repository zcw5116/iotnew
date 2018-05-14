package com.zyuc.stat.iotNBLiuzk.etl

import com.alibaba.fastjson.{JSON, JSONObject}
import com.zyuc.stat.iot.etl.util.MMEConverterUtils
import com.zyuc.stat.properties.ConfigProperties
import com.zyuc.stat.utils.FileUtils
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.{Logging, SparkConf, SparkContext}

import scala.collection.mutable

/**
  * Created by maoheng on 18-4-10 下午2:50.
  */
object MMELogETL_New extends Logging{
	def doJob(parentContext: SQLContext, fileSystem: FileSystem, params: JSONObject): String = {
		val sqlContext = parentContext.newSession()
		//sqlContext.sql("use " + ConfigProperties.IOT_HIVE_DATABASE)
		
		val appName      = params.getString("appName")
		val loadTime     = params.getString("loadTime")
		val inputPath    = params.getString("inputPath")
		val outputPath   = params.getString("outputPath")
		val hwmmWildcard = params.getString("hwmmWildcard")
		val hwsmWildcard = params.getString("hwsmWildcard")
		val ztmmWildcard = params.getString("ztmmWildcard")
		val ztsmWildcard = params.getString("ztsmWildcard")
		val ermmWildcard = params.getString("ermmWildcard")
		val ersmWildcard = params.getString("ersmWildcard")

		val provinceArr  = params.getString("provinceList").split(" |,")
		
		val wildcardMap: mutable.Map[String, String] = mutable.Map()
		wildcardMap += (hwmmWildcard -> MMEConverterUtils.NB_MME_HWMM_TYPE)
		wildcardMap += (hwsmWildcard -> MMEConverterUtils.NB_MME_HWSM_TYPE)
		wildcardMap += (ztmmWildcard -> MMEConverterUtils.NB_MME_ZTMM_TYPE)
		wildcardMap += (ztsmWildcard -> MMEConverterUtils.NB_MME_ZTSM_TYPE)
		wildcardMap += (ermmWildcard -> MMEConverterUtils.NB_MME_ERMM_TYPE)
		wildcardMap += (ersmWildcard -> MMEConverterUtils.NB_MME_ERSM_TYPE)
		
		var unionDF: DataFrame = null
		var findFile: Boolean = false
		
		val partitionArr = Array("d", "h", "m5")
		
		def getTemplet: String = {
			var templet = ""
			partitionArr.foreach(par => {templet = templet + "/" + par + "=*"})
			
			templet
		}
		
		val dirTodo  = inputPath + "/" + loadTime
		val dirDoing = inputPath + "/" + loadTime + "_doing"
		val dirDone  = inputPath + "/" + loadTime + "_done"
		var isRenamed: Boolean = false
		var result: String = null
		
		// 目录改名为doing后缀
		isRenamed = FileUtils.renameHDFSDir(fileSystem, dirTodo, dirDoing)
		result = if (isRenamed) "Success" else "Failed"
		logInfo(s"$result to rename $dirTodo to $dirDoing")
		if (!isRenamed) {
			return s"appName: $appName: $result to rename $dirTodo to $dirDoing"
		}
		
		try {
			provinceArr.foreach(prov => {
				wildcardMap.keys.foreach(wildcard => {
					val mmeType = wildcardMap(wildcard)
					val fileWildcard = dirDoing + "/" + prov + "/*" + wildcard + "*"
					
					if (FileUtils.getFilesByWildcard(fileSystem, fileWildcard).length > 0) {
						findFile = true
						val fileDF = sqlContext.read.format("json").load(fileWildcard)
						val newDF  = MMEConverterUtils.parseMME(fileDF, mmeType)
						if (newDF != null) {
							unionDF = if (unionDF == null) newDF else unionDF.unionAll(newDF)
						}
					}
				})
			})
			
			if (!findFile) {
				logInfo(s"No file found during time: $loadTime")
				return s"appName: $appName: No file found from path $inputPath"
			}
			
			val tempDestDir = outputPath + "/temp/" + loadTime
			
			logInfo(s"Write data into $tempDestDir")
			unionDF.coalesce(1).write.mode(SaveMode.Overwrite).format("orc").partitionBy(partitionArr: _*)
				.save(tempDestDir)
			
			val fileParSet = new mutable.HashSet[String]()
			
			FileUtils.getFilesByWildcard(fileSystem, tempDestDir + getTemplet + "/*.orc")
				.foreach(outFile => {
					val tmpPath = outFile.getPath.toString
					val filePar = tmpPath.substring(0, tmpPath.lastIndexOf("/"))
						.replace(tempDestDir, "").substring(1)
					
					fileParSet     .+= (filePar)
				})
			
			logInfo(s"Move temp files")
			FileUtils.moveTempFiles(fileSystem, outputPath+"/", loadTime, getTemplet, fileParSet)
			
			// 维护HIVE表,判断是否要增加分区
			val hiveTab = "iot_mme_log"
			
			//chkHiveTabPartition(sqlContext, hiveTab, fileParSet)
		} catch {
			case e: Exception =>
				e.printStackTrace()
				val cleanPath = outputPath + "/temp/" + loadTime
				FileUtils.cleanFilesByPath(fileSystem, cleanPath)
				logError(s"[$appName] 失败  处理异常" + e.getMessage)
				s"appName: $appName: ETL Failed. "
		}
		
		// 目录改名为done后缀
		isRenamed = FileUtils.renameHDFSDir(fileSystem, dirDoing, dirDone)
		result = if (isRenamed) "Success" else "Failed"
		logInfo(s"$result to rename $dirDoing to $dirDone")
//		if (!isRenamed) {
//			return s"appName: $appName: $result to rename $dirDoing to $dirDone"
//		}
		
		s"appName: $appName: ETL Success. "
	}

	
	def chkHiveTabPartition(sqlContext: SQLContext, hiveTab: String,
	                        fileParSet: mutable.HashSet[String]): Unit = {
		fileParSet.foreach(par => {
			var d  = ""
			var h  = ""
			var m5 = ""
			
			par.split("/").foreach(subpath => {
				if (subpath.startsWith("d="))  d  = subpath.substring(2)
				if (subpath.startsWith("h="))  h  = subpath.substring(2)
				if (subpath.startsWith("m5=")) m5 = subpath.substring(3)
			})
			
			if (d.nonEmpty && h.nonEmpty && m5.nonEmpty) {
				val sql = s"alter table $hiveTab add if not exists " +
					s"partition(d='$d', h='$h', m5='$m5')"
				logInfo(s"Exec partition: $sql")
				sqlContext.sql(sql)
			}
		})
	}

	
	def main(args: Array[String]): Unit = {
		val sparkConf = new SparkConf().setMaster("local[2]").setAppName("mh_testMME")
		val sc = new SparkContext(sparkConf)
		val sqlContext = new HiveContext(sc)
		
		//sqlContext.sql("use " + ConfigProperties.IOT_HIVE_DATABASE)
		
		val paramsString: String =
			"""
			  |{
			  | "appName"      : "MHTEST",
			  | "loadTime"     : "201805091509",
			  | "inputPath"    : "hdfs://inmnmbd02:8020/user/epciot/data/mme/src/nb",
			  | "outputPath"   : "hdfs://inmnmbd02:8020/user/epciot/data/cdr/transform/nb",
			  | "hwmmWildcard" : "HuaweiUDN-MM",
			  | "hwsmWildcard" : "HuaweiUDN-SM",
			  | "ztmmWildcard" : "sgsnmme_mm",
			  | "ztsmWildcard" : "sgsnmme_sm",
				| "ermmWildcard" : "er_mm",
				| "ersmWildcard" : "er_sm",
			  | "provinceList" : "GSS,ZJ"
			  |}
			""".stripMargin
		//provinceList
		//GX,GZ,HaiN,HUN,SC,YN,CQ,AH,AHH,AHHH,AHHHH,AHHHHH,FJ,JX,HLJ,LN,NM1,NM2,SD,TJ
		//BJ,GD,GDD,GDDD,GDDDD,GDDDDD,HB,HN,HUB,JL,NX,QH,XZ,XJ,JS,JSS,SHANX,SH
		//SX,SXX,GS,GSS,ZJ,ZJJ,ZJJJ,ZJJJJ
		val params = JSON.parseObject(paramsString)
		val fileSystem = FileSystem.get(sc.hadoopConfiguration)
		val rst = doJob(sqlContext, fileSystem, params)
	}
}
