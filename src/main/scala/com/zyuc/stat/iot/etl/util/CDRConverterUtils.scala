package com.zyuc.stat.iot.etl.util

import org.apache.spark.Logging
import org.apache.spark.sql.DataFrame


/**
  * Created by zhoucw on 17-7-23.
  * 1. pgw: add bsid field
  */
object CDRConverterUtils extends Logging{
  val LOG_TYPE_PDSN:String = "pdsn"
  val LOG_TYPE_PGW:String = "pgw"
  val LOG_TYPE_HACCG:String = "haccg"

  def parse(df: DataFrame, authLogType:String) = {
    var newDF:DataFrame = null
    try{
      if(authLogType==LOG_TYPE_PDSN){
        newDF = df.selectExpr("MDN  as  mdn_2","T0  as  streamnumber","T1  as  acct_status_type",
          "T10  as  source_ipv6_prefix","T11  as  ipv6_interface_id","T12  as  account_session_id",
          "T13  as  correlation_id","T14  as  session_continue","T15  as  beginning_session",
          "T16  as  service_reference_id","T17  as  flow_id_parameter","T18  as  home_agent",
          "T19  as  pdsn_address","T2  as  roamflag","T20  as  serving_pcf","T21  as  bsid",
          "T22  as  foreign_agent_address","T23  as  subnet","T24  as  carrier_id","T25  as  user_zone",
          "T26  as  gmt_time_zone_offset","T27  as  service_option","T28  as  ip_technology",
          "T29  as  compulsory_tunnel_indicator","T3  as  paidtype","T30  as  release_indicator",
          "T31  as  always_on","T32  as  hot_line_accounting_indication","T33  as  flow_status",
          "T34  as  termination","T35  as  originating","T36  as  bad_ppp_frame_count","T37  as  event_time",
          "T38  as  active_time","T39  as  number_of_active_transitions","T4  as  mdn",
          "T40  as  in_bound_mobile_ip_signaling_octet_count","T41  as  outbound_mobile_ip_signaling_octet_count",
          "T42  as  last_user_activity_time","T43  as  filtered_terminating","T44  as  filtered_originating",
          "T45  as  rsvp_inbound_octet_count","T46  as  rsvp_outbound_octet_count","T47  as  rsvp_inbound_packet_count",
          "T48  as  rsvp_outbound_packet_count","T49  as  ip_quality_of_service","T5  as  msid",
          "T50  as  granted_qos_parameters","T51  as  container","T52  as  acct_authentic",
          "T53  as  acct_session_time","T54  as  acct_input_packets","T55  as  acct_output_packets",
          "T56  as  acct_terminate_cause","T57  as  acct_input_gigawords","T58  as  acct_output_gigawords",
          "T59  as  session_id","T6  as  esn","T60  as  chteprl_otherarea_access_id","T61  as  pmip_indicator",
          "T62  as  ip_services_authorized","T63  as  pdsn_ipv6_address","T64  as  home_agent_ipv6_address",
          "T65  as  foreign_agent_ipv6_address","T66  as  pcf_ipv6_address","T7  as  meid","T8  as  source_ip_address",
          "T800  as  acce_province","T801  as  acce_region","T802  as  sid","T803  as  nid","T804  as  siteid",
          "T805  as  sitename","T806  as  cellid","T807  as  cellname","T808  as  secid","T809  as  enterprise_name",
          "T9  as  nai", "substr(regexp_replace(T37,'-',''),3,6) as d", "substr(T37,12,2) as h",
          "lpad(floor(substr(T37,15,2)/5)*5,2,'0') as m5")
      }
      else if(authLogType==LOG_TYPE_PGW){
        newDF = df.selectExpr("IMSI  as  imsi", "MDN  as  mdn", "T0  as  recordtype", "T1  as  servedimsi",
          "T10  as  p_gwplmnidentifier", "T11  as  accesspointnameni", "T12  as  pdppdntype",
          "T13  as  servedpdppdnaddress", "T14  as  servedpdppdnaddressext", "T15  as  dynamicaddressflag",
          "T16  as  dynamicaddressflagext", "T18  as  mstimezone", "T19  as  duration",
          "T2  as  imsiunauthenticatedflag", "T20  as  causeforrecclosing", "T21  as  diagnostics",
          "T22  as  recordsequencenumber", "T23  as  nodeid", "T24  as  recordextensions",
          "T25  as  localsequencenumber", "T26  as  apnselectionmode", "T27  as  servedmsisdn",
          "T28  as  userlocationinformation", "T29  as  usercsginformation", "T3  as  servedimeisv",
          "T30  as  threegpp2userlocationinformation", "T31  as  chargingcharacteristics",
          "T32  as  chchselectionmode", "T33  as  imssignalingcontext", "T34  as  servingnodeplmnidentifier",
          "T35  as  rattype", "T36  as  psfurnishcharginginformation", "T37  as  starttime", "T38  as  stoptime",
          "T39  as  camelcharginginformation", "T4  as  served3gpp2meid", "T40  as  externalchargingid",
          "T41  as  servedmnnai", "T42  as  l_ratinggroup", "T43  as  l_chargingrulebasename",
          "T44  as  l_localsequencenumber", "T45  as  l_timeoffirstusage", "T46  as  l_timeoflastusage",
          "T47  as  l_timeusage", "T48  as  l_serviceconditionchange", "T49  as  l_qosinformationneg",
          "T5  as  p_gwaddress", "T50  as  l_datavolumefbcuplink", "T51  as  l_datavolumefbcdownlink",
          "T52  as  l_timeofreport", "T53  as  sc_qosnegotiated", "T54  as  sc_datavolumegprsuplink",
          "T55  as  sc_datavolumegprsdownlink", "T56  as  sc_changecondition", "T57  as  sc_changetime",
          "T6  as  chargingid", "T7  as  pdnconnectionid", "T8  as  servingnodeaddress", "T800  as  t800",
          "T801  as  t801", "T802  as  t802", "T804  as  t804", "T805  as  t805", "T806  as  t806",
          "T807  as  t807", "T809  as  t809", "T9  as  servingnodetype",
          "regexp_extract(T28, 'eNodeBId=([0-9]+)', 1) as bsid",
          "substr(regexp_replace(T46,'-',''),3,6) as d", "substr(T46,12,2) as h",
          "lpad(floor(substr(T46,15,2)/5)*5,2,'0') as m5")

        newDF = newDF.filter(newDF("l_timeoflastusage").isNotNull)
      }
      else if(authLogType==LOG_TYPE_HACCG){
        newDF = df.selectExpr("MDN  as  mdn_2","T0  as  streamnumber","T1  as  acct_status_type",
          "T10  as  source_ipv6_prefix","T11  as  ipv6_interface_id","T12  as  account_session_id",
          "T13  as  correlation_id","T14  as  session_continue","T15  as  beginning_session",
          "T16  as  service_reference_id","T17  as  flow_id_parameter","T18  as  home_agent",
          "T19  as  pdsn_address","T2  as  roamflag","T20  as  serving_pcf","T21  as  bsid",
          "T22  as  foreign_agent_address","T23  as  subnet","T24  as  carrier_id","T25  as  user_zone",
          "T26  as  gmt_time_zone_offset","T27  as  service_option","T28  as  ip_technology",
          "T29  as  compulsory_tunnel_indicator","T3  as  paidtype","T30  as  release_indicator",
          "T31  as  always_on","T32  as  hot_line_accounting_indication","T33  as  flow_status",
          "T34  as  termination","T35  as  originating","T36  as  bad_ppp_frame_count","T37  as  event_time",
          "T38  as  active_time","T39  as  number_of_active_transitions","T4  as  mdn",
          "T40  as  in_bound_mobile_ip_signaling_octet_count","T41  as  outbound_mobile_ip_signaling_octet_count",
          "T42  as  last_user_activity_time","T43  as  filtered_terminating","T44  as  filtered_originating",
          "T45  as  rsvp_inbound_octet_count","T46  as  rsvp_outbound_octet_count","T47  as  rsvp_inbound_packet_count",
          "T48  as  rsvp_outbound_packet_count","T49  as  ip_quality_of_service","T5  as  msid",
          "T50  as  granted_qos_parameters","T51  as  container","T52  as  acct_authentic",
          "T53  as  acct_session_time","T54  as  acct_input_packets","T55  as  acct_output_packets",
          "T56  as  acct_terminate_cause","T57  as  acct_input_gigawords","T58  as  acct_output_gigawords",
          "T59  as  session_id","T6  as  esn","T60  as  chteprl_otherarea_access_id","T61  as  pmip_indicator",
          "T62  as  ip_services_authorized","T63  as  pdsn_ipv6_address","T64  as  home_agent_ipv6_address",
          "T65  as  foreign_agent_ipv6_address","T7  as  meid","T8  as  source_ip_address",
          "T800  as  acce_province","T801  as  acce_region","T802  as  sid","T803  as  nid","T804  as  siteid",
          "T805  as  sitename","T806  as  cellid","T807  as  cellname","T808  as  secid","T809  as  enterprise_name",
          "T9  as  nai", "substr(regexp_replace(T37,'-',''),3,6) as d", "substr(T37,12,2) as h",
          "lpad(floor(substr(T37,15,2)/5)*5,2,'0') as m5")
      }
    }catch {
      case e:Exception =>{
        e.printStackTrace()
        logError(s"[ $authLogType ] 失败 处理异常 " + e.getMessage)
      }
    }
    newDF
  }
}
