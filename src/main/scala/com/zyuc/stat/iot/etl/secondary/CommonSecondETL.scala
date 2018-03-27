package com.zyuc.stat.iot.etl.secondary

import com.zyuc.stat.iot.etl.util.CommonETLUtils
import com.zyuc.stat.properties.ConfigProperties
import com.zyuc.stat.utils.FileUtils.makeCoalesce
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{Logging, SparkConf, SparkContext}

/**
  * Created by limm on 2017/9/14.
  */
object CommonSecondETL extends Logging{
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    sqlContext.sql("use "+ ConfigProperties.IOT_HIVE_DATABASE)
    sqlContext.setConf("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
    //  sqlContext.sql("use iot")
    val fileSystem = FileSystem.get(sc.hadoopConfiguration)


    // 入参处理
    //
    val appName = sc.getConf.get("spark.app.name") // name_{}_2017073111
    val inputPath = sc.getConf.get("spark.app.inputPath") // "hdfs://EPC-LOG-NM-15:8020/hadoop/IOT/data/cdr/output/pdsn/data/"
    val outputPath = sc.getConf.get("spark.app.outputPath") //"hdfs://EPC-LOG-NM-15:8020/hadoop/IOT/data/cdr/secondaryoutput/pdsn/"
    val itemType = sc.getConf.get("spark.app.item.type") // pdsn,pgw,haccg   iot_cdr_data_haccg iot_mme_data_nb
    val FirstETLTable = sc.getConf.get("spark.app.table.source") // "iot_cdr_data_pdsn"
    val SecondaryETLTable = sc.getConf.get("spark.app.table.stored") // "iot_cdr_data_pdsn_h"
    val timeid = sc.getConf.get("spark.app.timeid")//yyyymmddhhmiss
    val cyctype = sc.getConf.get("spark.app.interval")
    val terminaltable = sc.getConf.get("spark.app.table.terminal")
    val coalesceSize = sc.getConf.get("spark.app.coalesce.size").toInt //128
    val vpnToApnMapFile = sc.getConf.get("spark.app.vpnToApnMapFile", "/hadoop/IOT/ANALY_PLATFORM/BasicData/VpdnToApn/vpdntoapn.txt")
    //入参打印
    logInfo("##########--inputPath:   " + inputPath)
    logInfo("##########--outputPath:  " + outputPath)
    logInfo("##########--storedtable: " + SecondaryETLTable)
    logInfo("##########--itemType:    " + itemType)
    logInfo("##########--timeid:      " + timeid)
    logInfo("##########--cyctype:     " + cyctype)




    // 清洗粒度 d,h
    val hourid = timeid.substring(0,10)
    val dayid  = timeid.substring(0,8)
    val partitionD = timeid.substring(2, 8)
    val partitionH = timeid.substring(8,10)
    //itemType

    logInfo("##########--partitionD: " + partitionD)
    logInfo("##########--partitionH: " + partitionH)
    val ItmeName = itemType.split("_",2)(0)
    //val subItmeName = itemType.split("_",2)(1)


    logInfo("##########--ItmeName:" + ItmeName)
    //logInfo("##########--subItmeName:  " + subItmeName)
    // 待清洗文件路径
    var partitions:String = null
    var sourceinputPath:String = null
    var subPath:String = null
    if(cyctype == "d"){
      partitions = "d"
      subPath = dayid
      sourceinputPath =  inputPath + "/d=" + partitionD + "/"
    }else if (cyctype == "h"){
      partitions = "d,h"
      subPath = hourid
      if(ItmeName=="auth"){
        sourceinputPath = inputPath + "/dayid=" + dayid + "/hourid=" + partitionH + "/"
      }else{
        sourceinputPath = inputPath + "/d=" + partitionD + "/h=" + partitionH + "/"
      }
    }

    logInfo("##########--sourceinputPath:   " + sourceinputPath)

    // 字段处理
    val tmptable = "tmpsourceTab"
    sqlContext.read.format("orc").load(sourceinputPath).registerTempTable(tmptable)
    var resultDF:DataFrame = null
    logInfo("##########--DealSourceDate   ")
    resultDF = DealSourceDate(sqlContext,tmptable,itemType,cyctype,partitionD,partitionH,terminaltable,vpnToApnMapFile)
    //resultDF.show(5)
    if(resultDF == null){
      logInfo("##########--ERROR:resultDF is null,please check it...  " )
      return
    }

    // 文件切片
    val coalesceNum = makeCoalesce(fileSystem, sourceinputPath, coalesceSize)

    // 将数据存入到HDFS， 并刷新分区表

    logInfo("##########--subPath:" + subPath)
    CommonETLUtils.saveDFtoPartition(sqlContext, fileSystem, resultDF, coalesceNum, partitions,subPath, outputPath, SecondaryETLTable, appName)
    logInfo("##########--subPath:" + subPath)

  }



  def DealSourceDate(sqlContext:SQLContext, tmptable:String, itemType:String, cyctype:String, partitionD:String, partitionH:String, terminaltable:String,vpnToApnMapFile:String): DataFrame ={


    var resultDF:DataFrame = null
    var userDF:DataFrame = null
    // authlog_3g
    logInfo("##########--type and cyc is:  "+itemType + "--" + cyctype)
    if(itemType=="auth_3g"){
      if(cyctype =="h") {

        resultDF = sqlContext.sql(
          s""" select auth_result,auth_time,device,
             |       imsicdma,imsilte,mdn,nai_sercode,nasport,nasportid,
             |       nasporttype,pcfip,srcip,
             |       case when auth_result = '0' then 'success' else 'failed' end as result,
             |       regexp_replace(auth_time,"[: -]", "") as authtime,
             |       '${partitionD}' as d,'${partitionH}' as h
             |from   ${tmptable}
           """.stripMargin
        )
        //resultDF.selectExpr("auth_result","auth_time","device","imsicdma","imsilte","mdn","nai_sercode","nasport","nasportid","nasporttype",
        //  "pcfip","srcip","",""
        //).withColumn()
       // resultDF = sourceDF.select(sourceDF.col("auth_result"), sourceDF.col("device"), sourceDF.col("imsicdma"),
       //   sourceDF.col("imsilte"), sourceDF.col("mdn"), sourceDF.col("nai_sercode"), sourceDF.col("nasport"),
       //   sourceDF.col("nasportid"), sourceDF.col("nasporttype"), sourceDF.col("pcfip"), sourceDF.col("srcip")).
       //   withColumn("authtime", regexp_replace(col("auth_time"), "[: -]", "")).
       //   withColumn("result", when(col("auth_result") === 0, "success").otherwise("failed")).
       //   withColumn("d", lit(partitionD)).withColumn("h", lit(partitionH))
      }

      if(cyctype =="d"){
        resultDF = sqlContext.sql(
          s""" select auth_result,auth_time,device,
             |       imsicdma,imsilte,mdn,nai_sercode,nasport,nasportid,
             |       nasporttype,pcfip,srcip,
             |       result,
             |       authtime,
             |       '${partitionD}' as d
             |from   ${tmptable}
           """.stripMargin
        )
        //resultDF = sourceDF.select(sourceDF.col("auth_result"), sourceDF.col("device"), sourceDF.col("imsicdma"),sourceDF.col("authtime"),
        //  sourceDF.col("imsilte"), sourceDF.col("mdn"), sourceDF.col("nai_sercode"), sourceDF.col("nasport"),
        //  sourceDF.col("nasportid"), sourceDF.col("nasporttype"), sourceDF.col("pcfip"), sourceDF.col("srcip"),
        //  sourceDF.col("result")).withColumn("d", lit(partitionD))
//
      }
    }
    // authlog_4g

    if(itemType=="auth_4g"){
      if(cyctype =="h") {

        resultDF = sqlContext.sql(
          s""" select auth_result,auth_time,device ,imsicdma ,imsilte,
             |       mdn,nai_sercode,nasport,nasportid,nasporttype,pcfip,
             |       case when auth_result = '0' then 'success' else 'failed' end as result,
             |       regexp_replace(auth_time,"[: -]", "") as authtime,
             |       '${partitionD}' as d,'${partitionH}' as h
             |from   ${tmptable}
           """.stripMargin
        )
        //resultDF = sourceDF.select(sourceDF.col("auth_result"), sourceDF.col("device"), sourceDF.col("imsicdma"),
        //  sourceDF.col("imsilte"), sourceDF.col("mdn"), sourceDF.col("nai_sercode"), sourceDF.col("nasportid"),
        //  sourceDF.col("nasporttype"), sourceDF.col("pcfip")).
        //  withColumn("authtime", regexp_replace(col("auth_time"), "[: -]", "")).
        //  withColumn("result", when(col("auth_result") === 0, "success").otherwise("failed")).
        //  withColumn("d", lit(partitionD)).withColumn("h", lit(partitionH))
      }

      if(cyctype =="d"){
        resultDF = sqlContext.sql(
          s""" select auth_result,auth_time,device ,imsicdma ,imsilte,
             |       mdn,nai_sercode,nasport,nasportid,nasporttype,pcfip,
             |       result,
             |       authtime,
             |       '${partitionD}' as d,'${partitionH}' as h
             |from   ${tmptable}
           """.stripMargin
        )
       // resultDF = sourceDF.
       //   select(sourceDF.col("auth_result"), sourceDF.col("authtime"), sourceDF.col("device"), sourceDF.col("imsicdma"),
       //     sourceDF.col("imsilte"), sourceDF.col("mdn"), sourceDF.col("nai_sercode"),
       //     sourceDF.col("nasportid"), sourceDF.col("nasporttype"), sourceDF.col("pcfip"),
       //     sourceDF.col("result")).withColumn("d", lit(partitionD))

      }



    }

    // authlog_vpdn
    if(itemType=="auth_vpdn"){
      if(cyctype =="h") {
        resultDF = sqlContext.sql(
          s""" select auth_result,auth_time,device ,entname,imsicdma ,imsilte,
             |       lnsip,mdn,nai_sercode,pdsnip,
             |       case when auth_result = '1' then 'success' else 'failed' end as result,
             |       regexp_replace(auth_time,"[: -]", "") as authtime,
             |       '${partitionD}' as d,'${partitionH}' as h
             |from   ${tmptable}
           """.stripMargin
        )
       // resultDF = sourceDF.select(sourceDF.col("auth_result"), sourceDF.col("device"), sourceDF.col("imsicdma"),
       //   sourceDF.col("imsilte"), sourceDF.col("mdn"), sourceDF.col("nai_sercode"), sourceDF.col("entname"),
       //   sourceDF.col("lnsip"), sourceDF.col("pdsnip")).
       //   withColumn("authtime", regexp_replace(col("auth_time"), "[: -]", "")).
       //   withColumn("result", when(col("auth_result") === 0, "success").otherwise("failed")).
       //   withColumn("d", lit(partitionD)).withColumn("h", lit(partitionH))
      }

      if(cyctype =="d"){
        resultDF = sqlContext.sql(
          s""" select auth_result,auth_time,device ,entname,imsicdma ,imsilte,
             |       lnsip,mdn,nai_sercode,pdsnip,
             |       result,
             |       authtime,
             |       '${partitionD}' as d
             |from   ${tmptable}
           """.stripMargin
        )
        //resultDF = sourceDF.
        //  select(sourceDF.col("auth_result"), sourceDF.col("authtime"), sourceDF.col("device"), sourceDF.col("imsicdma"),
        //    sourceDF.col("imsilte"), sourceDF.col("mdn"), sourceDF.col("nai_sercode"),
        //    sourceDF.col("entname"), sourceDF.col("lnsip"), sourceDF.col("pdsnip"),
        //    sourceDF.col("result")).withColumn("d", lit(partitionD))
//
      }

    }
    // mme
    if(itemType=="mme"){
      //terminaltable = "iot_dim_terminal"



      if(cyctype =="h") {

        resultDF = sqlContext.sql(
          s""" select m.procedureid,m.acctype,m.imsi,
             |       m.msisdn,m.sergw,m.pcause,m.imei,m.ci,m.enbid,m.uemme,m.newgrpid,
             |       m.newmmecode,m.newmtmsi,m.oldmcc,m.oldgrpid,m.oldmmecode,
             |       m.oldmtmsi,m.province,m.mmetype,m.result,m.isattach,
             |       t.tac,t.modelname,t.devicetype,
             |       substr(regexp_replace(starttime,"[: -]", ""),1,14) as starttime,
             |       '${partitionD}' as d,'${partitionH}' as h
             |from   ${tmptable} m
             |left join ${terminaltable} t
             |on   substr(m.imei,1,8) = t.tac
           """.stripMargin
        )

        //val terminalDF = sqlContext.table(terminaltable).select("tac", "modelname", "devicetype").cache()
        //resultDF = sourceDF.join(terminalDF, sourceDF.col("imei").substr(0, 8) === terminalDF.col("tac"), "left").
        //  select(sourceDF.col("msisdn").as("mdn"), sourceDF.col("province"), sourceDF.col("imei"), sourceDF.col("procedureid"),
        //    sourceDF.col("acctype"), sourceDF.col("imsi"), sourceDF.col("uemme"),
        //    sourceDF.col("pcause"), sourceDF.col("ci"), sourceDF.col("enbid"), sourceDF.col("uemme"),
        //    sourceDF.col("newgrpid"), sourceDF.col("newmmecode"), sourceDF.col("newmtmsi"), sourceDF.col("mmetype"),
        //    sourceDF.col("result"),
        //    terminalDF.col("tac"), terminalDF.col("modelname"), terminalDF.col("devicetype")
        //  ).withColumn("starttime", regexp_replace(col("starttime"), "[: -]", "").substr(0,14)).
        //  withColumn("d", lit(partitionD)).withColumn("h", lit(partitionH))
      }
      if(cyctype =="d"){
        resultDF = sqlContext.sql(
          s""" select m.procedureid,m.starttime,m.acctype,m.imsi,
             |       m.msisdn,m.sergw,m.pcause,m.imei,m.ci,m.enbid,m.uemme,m.newgrpid,
             |       m.newmmecode,m.newmtmsi,m.oldmcc,m.oldgrpid,m.oldmmecode,
             |       m.oldmtmsi,m.province,m.mmetype,m.result,m.isattach,
             |       m.tac,m.modelname,m.devicetype,
             |       '${partitionD}' as d
             |from   ${tmptable} m
           """.stripMargin
        )
       // resultDF = sourceDF.
       //   select(sourceDF.col("mdn"), sourceDF.col("province"), sourceDF.col("imei"), sourceDF.col("procedureid"),
       //     sourceDF.col("starttime"), sourceDF.col("acctype"), sourceDF.col("imsi"), sourceDF.col("uemme"),
       //     sourceDF.col("pcause"), sourceDF.col("ci"), sourceDF.col("enbid"), sourceDF.col("uemme"),
       //     sourceDF.col("newgrpid"), sourceDF.col("newmmecode"), sourceDF.col("newmtmsi"), sourceDF.col("mmetype"),
       //     sourceDF.col("result"), sourceDF.col("tac"), sourceDF.col("modelname"), sourceDF.col("devicetype")
       //   ).withColumn("d", lit(partitionD))

      }

    }
    if(itemType=="nbmme"){
      //terminaltable = "iot_dim_terminal"



      if(cyctype =="h") {

        resultDF = sqlContext.sql(
          s""" select m.procedureid,m.acctype,m.imsi,
             |       m.msisdn,m.sergw,m.pcause,m.imei,m.ci,m.enbid,m.uemme,m.newgrpid,
             |       m.newmmecode,m.newmtmsi,m.oldmcc,m.oldgrpid,m.oldmmecode,
             |       m.oldmtmsi,m.province,m.mmetype,m.result,m.isattach,
             |       t.tac,t.modelname,t.devicetype,
             |       substr(regexp_replace(starttime,"[: -]", ""),1,14) as starttime,
             |       RegProSign,RegProRst,delay,
             |       '${partitionD}' as d,'${partitionH}' as h
             |from   ${tmptable} m
             |left join ${terminaltable} t
             |on   substr(m.imei,1,8) = t.tac
           """.stripMargin
        )

      }
      //if(cyctype =="d"){
      //  resultDF = sqlContext.sql(
      //    s""" select m.procedureid,m.starttime,m.acctype,m.imsi,
      //       |       m.msisdn,m.sergw,m.pcause,m.imei,m.ci,m.enbid,m.uemme,m.newgrpid,
      //       |       m.newmmecode,m.newmtmsi,m.oldmcc,m.oldgrpid,m.oldmmecode,
      //       |       m.oldmtmsi,m.province,m.mmetype,m.result,m.isattach,
      //       |       m.tac,m.modelname,m.devicetype,
      //       |       '${partitionD}' as d
      //       |from   ${tmptable} m
      //     """.stripMargin
      //  )
//
      //}

    }


    // cdr_pgw
    if(itemType=="cdr_pgw"){
      import sqlContext.implicits._
      val vpnToApnDF  = sqlContext.read.format("text").load(vpnToApnMapFile).map(x=>x.getString(0).split(",")).map(x=>(x(0),x(1))).toDF("vpdndomain","apn")

      if(cyctype =="h"){

        resultDF = sqlContext.sql(
          s""" select imsi, mdn,recordtype, servingnodetype, bsid,starttime,stoptime,
             |       l_timeoffirstusage , l_timeoflastusage,l_datavolumefbcuplink as upflow,
             |       l_datavolumefbcdownlink as downflow,
             |        servedimsi, p_gwplmnidentifier, accesspointnameni, pdppdntype, servedpdppdnaddress,
             |       servedpdppdnaddressext, dynamicaddressflag, dynamicaddressflagext, mstimezone,
             |       duration, imsiunauthenticatedflag, causeforrecclosing, diagnostics, recordsequencenumber, nodeid,
             |       recordextensions, localsequencenumber, apnselectionmode, servedmsisdn,
             |       userlocationinformation, usercsginformation, servedimeisv, threegpp2userlocationinformation,
             |       chargingcharacteristics, chchselectionmode, imssignalingcontext, servingnodeplmnidentifier, rattype,
             |       psfurnishcharginginformation,   camelcharginginformation,
             |       served3gpp2meid, externalchargingid, servedmnnai,
             |       l_ratinggroup, l_chargingrulebasename, l_localsequencenumber,
             |        l_timeusage, l_serviceconditionchange,
             |       l_qosinformationneg, p_gwaddress,   l_timeofreport,
             |       sc_qosnegotiated, sc_datavolumegprsuplink, sc_datavolumegprsdownlink, sc_changecondition, sc_changetime,
             |       chargingid, pdnconnectionid, servingnodeaddress,
             |       t800, t801, t802, t804, t805, t806, t807, t809,
             |       '${partitionD}' as d,'${partitionH}' as h
             |from   ${tmptable}
           """.stripMargin
        )
        //resultDF =  sourceDF.select(sourceDF.col("mdn"),sourceDF.col("recordtype"),sourceDF.col("starttime"),sourceDF.col("stoptime"),
        //  sourceDF.col("l_timeoffirstusage"),sourceDF.col("l_timeoflastusage"),sourceDF.col("l_datavolumefbcuplink").alias("upflow"),
        //  sourceDF.col("l_datavolumefbcdownlink").alias("downflow"),sourceDF.col("l_datavolumefbcdownlink").alias("downflow")
        //).withColumn("d", lit(partitionD)).withColumn("h", lit(partitionH))

      }
      if(cyctype =="d"){
        resultDF = sqlContext.sql(
          s""" select imsi, mdn,recordtype, servingnodetype, bsid,starttime,stoptime,
             |       l_timeoffirstusage , l_timeoflastusage,upflow,
             |       downflow,
             |       servedimsi, p_gwplmnidentifier, accesspointnameni, pdppdntype, servedpdppdnaddress,
             |       servedpdppdnaddressext, dynamicaddressflag, dynamicaddressflagext, mstimezone,
             |       duration, imsiunauthenticatedflag, causeforrecclosing, diagnostics, recordsequencenumber, nodeid,
             |       recordextensions, localsequencenumber, apnselectionmode, servedmsisdn,
             |       userlocationinformation, usercsginformation, servedimeisv, threegpp2userlocationinformation,
             |       chargingcharacteristics, chchselectionmode, imssignalingcontext, servingnodeplmnidentifier, rattype,
             |       psfurnishcharginginformation,   camelcharginginformation,
             |       served3gpp2meid, externalchargingid, servedmnnai,
             |       l_ratinggroup, l_chargingrulebasename, l_localsequencenumber,
             |        l_timeusage, l_serviceconditionchange,
             |       l_qosinformationneg, p_gwaddress,   l_timeofreport,
             |       sc_qosnegotiated, sc_datavolumegprsuplink, sc_datavolumegprsdownlink, sc_changecondition, sc_changetime,
             |       chargingid, pdnconnectionid, servingnodeaddress,
             |       t800, t801, t802, t804, t805, t806, t807, t809,
             |       '${partitionD}' as d
             |from   ${tmptable}
           """.stripMargin
        )
        //resultDF =sourceDF.select(sourceDF.col("mdn"),sourceDF.col("recordtype"),sourceDF.col("starttime"),sourceDF.col("stoptime"),
        //  sourceDF.col("l_timeoffirstusage"),sourceDF.col("l_timeoflastusage"),sourceDF.col("upflow"),
        //  sourceDF.col("downflow")
        //).withColumn("d", lit(partitionD))

      }

    }

    // cdr_pdsn
    if(itemType=="cdr_pdsn"){

      if(cyctype =="h"){
        resultDF = sqlContext.sql(
          s""" select mdn_2,bsid,mdn,msid,nai,
             |       streamnumber,acct_status_type,
             |       source_ipv6_prefix,ipv6_interface_id,account_session_id,
             |       correlation_id,session_continue,beginning_session,service_reference_id,
             |       flow_id_parameter,home_agent,pdsn_address,roamflag,
             |       serving_pcf,foreign_agent_address,subnet,carrier_id,
             |       user_zone,gmt_time_zone_offset,service_option,ip_technology,
             |       compulsory_tunnel_indicator,paidtype,release_indicator,
             |       always_on,hot_line_accounting_indication,flow_status,
             |       termination,originating,bad_ppp_frame_count,
             |       event_time,active_time,number_of_active_transitions,
             |       in_bound_mobile_ip_signaling_octet_count,
             |       outbound_mobile_ip_signaling_octet_count,
             |       last_user_activity_time,filtered_terminating,filtered_originating,
             |       rsvp_inbound_octet_count,rsvp_outbound_octet_count,rsvp_inbound_packet_count,rsvp_outbound_packet_count,
             |       ip_quality_of_service,granted_qos_parameters,
             |       container,acct_authentic,acct_session_time,acct_input_packets,
             |       acct_output_packets,acct_terminate_cause,acct_input_gigawords,acct_output_gigawords,
             |       session_id,esn,chteprl_otherarea_access_id,
             |       pmip_indicator,ip_services_authorized,pdsn_ipv6_address,home_agent_ipv6_address,
             |       foreign_agent_ipv6_address,pcf_ipv6_address,meid,
             |       source_ip_address,acce_province,acce_region,sid,nid,siteid,
             |       sitename,cellid,cellname,secid,enterprise_name,
             |       '${partitionD}' as d,'${partitionH}' as h
             |from   ${tmptable}
           """.stripMargin
        )
       // resultDF = sourceDF.select(sourceDF.col("mdn"),sourceDF.col("account_session_id"),sourceDF.col("acct_status_type"),
       //   sourceDF.col("originating").alias("upflow"),sourceDF.col("termination").alias("downflow"),sourceDF.col("event_time"),sourceDF.col("active_time"),
       //   sourceDF.col("acct_input_packets"),sourceDF.col("acct_output_packets"),sourceDF.col("acct_session_time"),
       //   sourceDF.col("vpdncompanycode"),sourceDF.col("custprovince"),sourceDF.col("cellid"),sourceDF.col("bsid")).
       //   withColumn("d", lit(partitionD)).withColumn("h", lit(partitionH))
      }


      if(cyctype =="d"){
        resultDF = sqlContext.sql(
          s""" select mdn_2,bsid,mdn,msid,nai,
             |       streamnumber,acct_status_type,
             |       source_ipv6_prefix,ipv6_interface_id,account_session_id,
             |       correlation_id,session_continue,beginning_session,service_reference_id,
             |       flow_id_parameter,home_agent,pdsn_address,roamflag,
             |       serving_pcf,foreign_agent_address,subnet,carrier_id,
             |       user_zone,gmt_time_zone_offset,service_option,ip_technology,
             |       compulsory_tunnel_indicator,paidtype,release_indicator,
             |       always_on,hot_line_accounting_indication,flow_status,
             |       termination,originating,bad_ppp_frame_count,
             |       event_time,active_time,number_of_active_transitions,
             |       in_bound_mobile_ip_signaling_octet_count,
             |       outbound_mobile_ip_signaling_octet_count,
             |       last_user_activity_time,filtered_terminating,filtered_originating,
             |       rsvp_inbound_octet_count,rsvp_outbound_octet_count,rsvp_inbound_packet_count,rsvp_outbound_packet_count,
             |       ip_quality_of_service,granted_qos_parameters,
             |       container,acct_authentic,acct_session_time,acct_input_packets,
             |       acct_output_packets,acct_terminate_cause,acct_input_gigawords,acct_output_gigawords,
             |       session_id,esn,chteprl_otherarea_access_id,
             |       pmip_indicator,ip_services_authorized,pdsn_ipv6_address,home_agent_ipv6_address,
             |       foreign_agent_ipv6_address,pcf_ipv6_address,meid,
             |       source_ip_address,acce_province,acce_region,sid,nid,siteid,
             |       sitename,cellid,cellname,secid,enterprise_name,
             |       '${partitionD}' as d
             |from   ${tmptable}
           """.stripMargin
        )
        //resultDF = sourceDF.select(sourceDF.col("mdn"),sourceDF.col("account_session_id"),sourceDF.col("acct_status_type"),
        //  sourceDF.col("upflow"),sourceDF.col("downflow"),sourceDF.col("event_time"),sourceDF.col("active_time"),
        //  sourceDF.col("acct_input_packets"),sourceDF.col("acct_output_packets"),sourceDF.col("acct_session_time"),
        //  sourceDF.col("vpdncompanycode"),sourceDF.col("custprovince"),sourceDF.col("cellid"),sourceDF.col("bsid")).
        //  withColumn("d", lit(partitionD))

      }


    }

    if(itemType=="cdr_haccg"){

      resultDF = sqlContext.sql(
        s""" select mdn_2 , streamnumber , acct_status_type ,
           |       source_ipv6_prefix , ipv6_interface_id , account_session_id ,
           |       correlation_id , session_continue , beginning_session ,
           |       service_reference_id , flow_id_parameter , home_agent , pdsn_address ,
           |       roamflag , serving_pcf , bsid , foreign_agent_address ,
           |       subnet , carrier_id , user_zone , gmt_time_zone_offset ,
           |       service_option , ip_technology , compulsory_tunnel_indicator ,
           |       paidtype , release_indicator ,
           |       always_on , hot_line_accounting_indication ,
           |       flow_status , termination , originating ,
           |       bad_ppp_frame_count , event_time , active_time ,
           |       number_of_active_transitions , mdn ,
           |       in_bound_mobile_ip_signaling_octet_count ,
           |       outbound_mobile_ip_signaling_octet_count ,
           |       last_user_activity_time , filtered_terminating , filtered_originating ,
           |       rsvp_inbound_octet_count , rsvp_outbound_octet_count , rsvp_inbound_packet_count ,
           |       rsvp_outbound_packet_count , ip_quality_of_service , msid ,
           |       granted_qos_parameters , container , acct_authentic ,
           |       acct_session_time , acct_input_packets , acct_output_packets ,
           |       acct_terminate_cause , acct_input_gigawords , acct_output_gigawords ,
           |       session_id , esn , chteprl_otherarea_access_id , pmip_indicator ,
           |       ip_services_authorized , pdsn_ipv6_address , home_agent_ipv6_address ,
           |       foreign_agent_ipv6_address , meid , source_ip_address ,
           |       acce_province , acce_region , sid , nid ,
           |       siteid , sitename , cellid , cellname , secid , enterprise_name , nai,
           |       '${partitionD}' as d,'${partitionH}' as h
           |from   ${tmptable}
           """.stripMargin
      )
      //if(cyctype =="h"){
      //  resultDF = sourceDF.select(sourceDF.col("mdn"),sourceDF.col("account_session_id"),sourceDF.col("acct_status_type"),
      //    sourceDF.col("originating").alias("upflow"),sourceDF.col("termination").alias("downflow"),sourceDF.col("event_time"),sourceDF.col("active_time"),
      //    sourceDF.col("acct_input_packets"),sourceDF.col("acct_output_packets"),sourceDF.col("acct_session_time"),
      //    sourceDF.col("vpdncompanycode"),sourceDF.col("custprovince"),sourceDF.col("cellid"),sourceDF.col("bsid")).
      //    withColumn("d", lit(partitionD)).withColumn("h", lit(partitionH))
      //}


      if(cyctype =="d"){
        resultDF = sqlContext.sql(
          s""" select mdn_2 , streamnumber , acct_status_type ,
             |       source_ipv6_prefix , ipv6_interface_id , account_session_id ,
             |       correlation_id , session_continue , beginning_session ,
             |       service_reference_id , flow_id_parameter , home_agent , pdsn_address ,
             |       roamflag , serving_pcf , bsid , foreign_agent_address ,
             |       subnet , carrier_id , user_zone , gmt_time_zone_offset ,
             |       service_option , ip_technology , compulsory_tunnel_indicator ,
             |       paidtype , release_indicator ,
             |       always_on , hot_line_accounting_indication ,
             |       flow_status , termination , originating ,
             |       bad_ppp_frame_count , event_time , active_time ,
             |       number_of_active_transitions , mdn ,
             |       in_bound_mobile_ip_signaling_octet_count ,
             |       outbound_mobile_ip_signaling_octet_count ,
             |       last_user_activity_time , filtered_terminating , filtered_originating ,
             |       rsvp_inbound_octet_count , rsvp_outbound_octet_count , rsvp_inbound_packet_count ,
             |       rsvp_outbound_packet_count , ip_quality_of_service , msid ,
             |       granted_qos_parameters , container , acct_authentic ,
             |       acct_session_time , acct_input_packets , acct_output_packets ,
             |       acct_terminate_cause , acct_input_gigawords , acct_output_gigawords ,
             |       session_id , esn , chteprl_otherarea_access_id , pmip_indicator ,
             |       ip_services_authorized , pdsn_ipv6_address , home_agent_ipv6_address ,
             |       foreign_agent_ipv6_address , meid , source_ip_address ,
             |       acce_province , acce_region , sid , nid ,
             |       siteid , sitename , cellid , cellname , secid , enterprise_name , nai,
             |       '${partitionD}' as d
             |from   ${tmptable}
           """.stripMargin
        )
        //resultDF = sourceDF.select(sourceDF.col("mdn"),sourceDF.col("account_session_id"),sourceDF.col("acct_status_type"),
        //  sourceDF.col("upflow"),sourceDF.col("downflow"),sourceDF.col("event_time"),sourceDF.col("active_time"),
        //  sourceDF.col("acct_input_packets"),sourceDF.col("acct_output_packets"),sourceDF.col("acct_session_time"),
        //  sourceDF.col("vpdncompanycode"),sourceDF.col("custprovince"),sourceDF.col("cellid"),sourceDF.col("bsid")).
        //  withColumn("d", lit(partitionD))

      }


    }
    resultDF.show(4)
    resultDF


  }


}
