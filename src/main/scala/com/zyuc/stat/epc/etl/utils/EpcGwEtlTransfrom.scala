package com.zyuc.stat.epc.etl.utils

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._


/**
  * Created by zhoucw on 下午1:56.
  */
object EpcGwEtlTransfrom {

  def parse(df:DataFrame, cdrType:String): DataFrame = {
    var newDF:DataFrame = null

    if(cdrType == "EPC_SGW") {
      newDF = df.selectExpr("regexp_replace(mdn,'^00','') as mdn", "substr(regexp_replace(mdn,'^00',''), 1, 9) as mdnsection","imsi", "t11 as changetime", "t16 as starttime",
        "t17 as duration", "from_unixtime(unix_timestamp(t16) + t17) as stoptime",
        "t800", "t804 as bsid")
      newDF = newDF.selectExpr("mdn", "mdnsection", "imsi", "changetime", "starttime",
        "duration", "stoptime","t800", "bsid", "substr(regexp_replace(stoptime,'-',''),3,6) as d", "substr(stoptime,12,2) as h",
        "lpad(floor(substr(stoptime,15,2)/5)*5,2,'0') as m5")
    }else if(cdrType == "EPC_PGW"){
      newDF = df.selectExpr("regexp_replace(mdn,'^00','') as mdn", "substr(regexp_replace(mdn,'^00',''), 1, 9) as mdnsection","imsi",
        "t3 as servedIMEISV", "T27 as servedMSISDN",
        "T8  as servingNodeAddress", "T35 as rATType", "T42 as l_ratingGroup", "T45 as l_timeOfFirstUsage",
        "T46 as l_timeOfLastUsage", "T50 as l_datavolumeFBCUplink", "T51 as l_datavolumeFBCDownlink",  "T804 as ENODEBID",
        "from_unixtime(unix_timestamp(t16) + t17) as stoptime",
        "t800", "t804 as bsid", "substr(regexp_replace(T46,'-',''),3,6) as d", "substr(T46,12,2) as h",
      "lpad(floor(substr(T46,15,2)/5)*5,2,'0') as m5")
    }

    newDF
  }
}
