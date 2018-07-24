//package com.iottest

package com.zyuc.stat.iot.analysis.common

import ...

/**
  * Created by hadoop on 18-7-17.
  */
class BsSectorAnalysis(inDir:String, outDir:String) {

  //data
  var now:Date = new Date()
  var nowDay:String = getDaysBefore(0)

  var inputDir: String = inDir + "/data" // "/user/iot_ete/data/cdr/transform/pgw/data"
  var tempSaveDir: String = outDir + "/dataTemp/d=" + nowDay // "/user/iot_ete/bs_sector/pgw/dataTemp/"
  var saveDir:String = outDir + "/data/d=" + nowDay

  var sparkConf:SparkConf = new SparkConf().setMaster("local[2]").setAppName("testIot")
  var sc:SparkContext = new SparkContext(sparkConf)
  var sqlContext:HiveContext = new HiveContext(sc)


  //method
  def getDaysBefore( interval: Int):String = {
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyMMdd")

    val cal: Calendar = Calendar.getInstance()
    cal.setTime(now);

    cal.add(Calendar.DATE, - interval)
    val beforeday = dateFormat.format(cal.getTime())
    beforeday
  }

  /*
    过去天的数据读到临时表后保存到  tempSaveDir +  "/" +  dayStr下面
  */
  def tempNodeBSectorInfoByDay( dayStr:String): Int= {

    // hdfs://sparkhost:8020/user/iot_ete/data/cdr/transform/pgw/data/d=180716/h=02/m5=00/201807160222-1.orc
    val inputpath = sc.getConf.get("spark.app.inputpath", inputDir + "/d=" + dayStr + "/*")


    var df:DataFrame = null
    try{
      df = sqlContext.read.format("orc").load(inputpath)

    }catch{
      case e:Exception => {
        //e.printStackTrace()
        println("inputpath: ", inputpath, "异常")
        return -1
      }
    }
    println("inputpath: ", inputpath)
    df.registerTempTable("NodeBSectorInfoTmp")
    //df.printSchema()
    //df.show()
    df.registerTempTable("NodeBSectorInfoTmp")

    //"/user/iot_ete/bs_sector/pgw/dataTemp/d=180716/180709"
    val outpath =  tempSaveDir + "/last/" + dayStr
    val outDF = sqlContext.sql("select  prov, t802, enbid, t806, max(l_timeoflastusage) as l_timeoflastusage from NodeBSectorInfoTmp group by  prov, t802, enbid, t806").coalesce(1)
    println("outpath: ", outpath, outDF.count())
    //outDF.show()
    outDF.write.format("orc").mode(SaveMode.Overwrite).save(outpath)
    sqlContext.dropTempTable("NodeBSectorInfoTmp")

    return 0
  }

   /*
     过去7天的数据保存到  tempSaveDir  下面
   */
  def tempNodeBSectorInfo(): Int = {

    for( i <- 1 to 7){
      println( "Value of i: " + i )
      var dayStr:String = getDaysBefore(i)
      var relt:Int = tempNodeBSectorInfoByDay(dayStr)
    }

    val inputpath = sc.getConf.get("spark.app.inputpath", tempSaveDir + "/last/*")
    var df:DataFrame = null
    try{

      df = sqlContext.read.format("orc").load(inputpath)

    }catch{
      case e:Exception => {
        //e.printStackTrace()
        println("inputpath: ", inputpath, "异常")
        return -1
      }
    }
    println("inputpath: ", inputpath, df.count())
    //df.show()
    df.registerTempTable("NodeBSectorInfoTmp")


    //"/user/iot_ete/bs_sector/pgw/dataTemp/d=180716/180709"
    val outpath:String =  tempSaveDir + "/now"
    val outDF = sqlContext.sql(
      """
         select t.prov, t.tac, t.bsid, t.sectid, t.firtusetime,  t.firtusetime as lastusetime, 0 as status
         from (
                select  prov , t802 as tac , enbid as bsid, t806 as sectid, max(l_timeoflastusage) as firtusetime
                from NodeBSectorInfoTmp
                group by  prov, t802, enbid, t806
           ) t
      """).coalesce(1)
    println("outpath: ", outpath, outDF.count())
    //outDF.printSchema()
    //outDF.show()
    outDF.write.format("orc").mode(SaveMode.Overwrite).save(outpath)


    sqlContext.dropTempTable("NodeBSectorInfoTmp")

    return 0
  }

  def saveNodeBSectorInfo(now_table:String,  before_table:String) = {

    sqlContext.sql("show tables").show()

    val outDF = sqlContext.sql(
      """
        select  x.prov, x.tac, x.bsid, x.sectid, x.firtusetime, x.lastusetime, x.status
        from (

        		select n.prov , n.tac , n.bsid, n.sectid,
        				case
        				   when b.status is null or b.status = 6 then n.firtusetime
                   else b.firtusetime
        				end as firtusetime,

        				n.lastusetime ,

        				case
        					when b.lastusetime is null or  ( datediff(current_date(),to_date(b.lastusetime)) < 8)
        						 then 0
        				  when datediff(current_date(),to_date(b.lastusetime)) >= 8  and  datediff(current_date(),to_date(b.lastusetime)) < 16
        						  then 1
        					when datediff(current_date(),to_date(b.lastusetime)) >= 16 and datediff(current_date(),to_date(b.lastusetime))  < 31
        						  then 2
        					when datediff(current_date(),to_date(b.lastusetime)) >= 31 and datediff(current_date(),to_date(b.lastusetime))  < 61
        						  then 3
        					when datediff(current_date(),to_date(b.lastusetime)) >= 61 and datediff(current_date(),to_date(b.lastusetime))  < 91
        						  then 4
        					when datediff(current_date(),to_date(b.lastusetime)) >= 91 and datediff(current_date(),to_date(b.lastusetime))  < 101
        						  then 5
        					else   6
        				end as status
        		from  """ + now_table + """ n left join """ + before_table + """  b  on (n.prov = b.prov and n.tac = b.tac and n.bsid = b.bsid and n.sectid = b.sectid)

         union all

        		select t.prov , t.tac , t.bsid, t.sectid, t.firtusetime, t.lastusetime,
        			   case
        					when datediff(current_date(),to_date(t.lastusetime)) < 8
        						 then 0
        					when datediff(current_date(),to_date(t.lastusetime)) >= 8  and  datediff(current_date(),to_date(t.lastusetime)) < 16
        						  then 1
        					when datediff(current_date(),to_date(t.lastusetime)) >= 16 and datediff(current_date(),to_date(t.lastusetime))  < 31
        						  then 2
        					when datediff(current_date(),to_date(t.lastusetime)) >= 31 and datediff(current_date(),to_date(t.lastusetime))  < 61
        						  then 3
        					when datediff(current_date(),to_date(t.lastusetime)) >= 61 and datediff(current_date(),to_date(t.lastusetime))  < 91
        						  then 4
        					when datediff(current_date(),to_date(t.lastusetime)) >= 91 and datediff(current_date(),to_date(t.lastusetime))  < 101
        						  then 5
        					else   6
        				end as status
        		from (
        			     select n.status n_status, b.*
        			     from """ + now_table + """ n right join """ + before_table + """  b   on (n.prov = b.prov and n.tac = b.tac and n.bsid = b.bsid and n.sectid = b.sectid)
        			   ) t
        		where t.n_status is  null

        ) x
        where (x.prov is null or x.prov != 'provtest') and (x.tac is null or x.tac != 'tactest') and
              (x.bsid is null or x.bsid != 'bsidtest') and (x.sectid is null or x.sectid != 'sectidtest')

      """
        ).coalesce(1)


    //outDF.printSchema()
    outDF.show()
    //"/user/iot_ete/bs_sector/pgw/data/d=180716/"
    println("outpath: ", saveDir, outDF.count())
    outDF.write.format("orc").mode(SaveMode.Overwrite).save(saveDir)
  }



  def loadData2Table(tableName:String, inputpath:String ):Int = {

    var df:DataFrame = null
    try{
       df = sqlContext.read.format("orc").load(inputpath)
    }catch{
      case e:Exception => {
        //e.printStackTrace()
        println("loadData2Table(" + tableName + "): " + inputpath, "异常")
        createEmptyTabe_T(tableName)
        return -1
      }
    }
    println("loadData2Table(" + tableName + "): " + inputpath,  df.count())
    df.registerTempTable(tableName)
    //df.printSchema()
    df.show()
    return 0
  }


  def createEmptyTabe_T(tabelName:String) = {

    val nameRDD = sc.makeRDD(Array(
      "{\"prov\":\"provtest\"," +
        "\"tac\":\"tactest\"," +
        "\"bsid\":\"bsidtest\"," +
        "\"sectid\":\"sectidtest\"," +
        "\"firtusetime\":\"firtusetimetest\"," +
        "\"lastusetime\":\"lastusetimetest\"," +
        "\"status\":\"0\"" +
        "}"
    ))
    val nameDF = sqlContext.read.json(nameRDD)
    nameDF.registerTempTable(tabelName)
    nameDF.printSchema()
    nameDF.show()

  }

  def data2Tib(tabelName:String) : Int = {

    var df:DataFrame = null
    try{
      df = sqlContext.read.format("orc").load(saveDir + "/*orc")

    }catch{
      case e:Exception => {
        //e.printStackTrace()
        println("inputpath( data2Tib ->" + tabelName  + "): "+  saveDir + "/*orc", "异常")
        return -1
      }
    }
    println("inputpath( data2Tib ->" + tabelName  + "): "+  saveDir + "/*orc")

    df.registerTempTable("data2Tib")

    val result = sqlContext.sql("""
           select case when prov is null then '-1'
                       else prov
                  end as prov,
                  case when tac is null then ''
                       else tac
                  end as tac,
                  case when bsid is null then ''
                       else bsid
                  end as bsid,
                  case when sectid is null then ''
                      else sectid
                  end as sectid,
                  firtusetime, lastusetime, status
           from data2Tib
     """ ).map(x=>(x.getString(0), x.getString(1), x.getString(2), x.getString(3), x.getString(4), x.getString(5), x.getInt(6))).collect()

    //val result =df.map(x=>(x.getString(0), x.getString(1), x.getString(2), x.getString(3), x.getString(4), x.getString(5), x.getInt(6))).collect()
    var dbConn = DbUtils.getDBConnection 
    dbConn.setAutoCommit(false)
    val sql =   "insert into " + tabelName +
      """ (prov,  tac,  bsid, sectid, firtusetime, lastusetime, status)
                   values (?, ?, ?, ?, ?, ?, ?)
                   on duplicate key update firtusetime=?, lastusetime=?, status=?
              """

    val pstmt = dbConn.prepareStatement(sql)

    var i = 0
    for(r<-result){
      var prov = r._1
      var tac = r._2
      var bsid = r._3
      var sectid = r._4

      val firtusetime = r._5
      val lastusetime = r._6
      val status = r._7

      pstmt.setString(1, prov)
      pstmt.setString(2, tac)
      pstmt.setString(3, bsid)
      pstmt.setString(4, sectid)
      pstmt.setString(5, firtusetime)
      pstmt.setString(6, lastusetime)
      pstmt.setInt(7, status)

      pstmt.setString(8, firtusetime)
      pstmt.setString(9, lastusetime)
      pstmt.setInt(10, status)

      pstmt.addBatch()
      if (i % 1000 == 0 ) {
        i = 0
        pstmt.executeBatch
        dbConn.commit()
      }

      i = i+ 1
    }
    pstmt.executeBatch
    dbConn.commit()
    pstmt.close()
    dbConn.close()
    i
  }


  def destructor() = {

    val fileSystem = FileSystem.get(sc.hadoopConfiguration)
    try{
      fileSystem.globStatus(new Path(tempSaveDir) ).foreach(x => fileSystem.delete(x.getPath(), true))
    }catch{
      case e:Exception => {
        //e.printStackTrace()
      }
    }

    sc.stop()
  }

}


object BsSectorAnalysisObj {
  def main(args: Array[String]) {

    var inDirArr =   Array("/user/iot_ete/data/cdr/transform/pgw",  "/user/iot/data/cdr/transform/nb")
    var outDirArr =  Array("/user/iot_ete/bs_sector/pgw",           "/user/iot_ete/bs_sector/nb")
    var tidbTableArr =   Array("iot_bs_sector_pgw",                      "iot_bs_sector_nb")

    for(i<- 0 until inDirArr.length) {
      //val inDir:String = "/user/iot_ete/data/cdr/transform/pgw/data/"
      //val outDir:String = "/user/iot_ete/bs_sector/pgw/data"

      val inDir:String = inDirArr(i)
      val outDir:String = outDirArr(i)
      val tidbTabel:String = tidbTableArr(i)


      val sectorObject = new BsSectorAnalysis(inDir, outDir)

      val nowDay:String = sectorObject.nowDay
      val lastDay:String = sectorObject.getDaysBefore(1)

      var relt = sectorObject.tempNodeBSectorInfo()

      var nowTable:String = "NodeBSectorInfo_now"
      var lastTable:String = "NodeBSectorInfo_last"
      sectorObject.loadData2Table(nowTable,  outDir + "/dataTemp/d=" + nowDay  + "/now/*orc")
      sectorObject.loadData2Table(lastTable, outDir + "/data/d="     + lastDay + "/*orc")
      sectorObject.saveNodeBSectorInfo(nowTable, lastTable)
      sectorObject.data2Tib(tidbTabel)

      sectorObject.destructor()  //sectorObject.sc.stop()
    }
  }
}
