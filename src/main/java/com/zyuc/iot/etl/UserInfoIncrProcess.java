package com.zyuc.iot.etl;

import com.zyuc.iot.config.ConfigProperties;
import com.zyuc.iot.etl.util.ProvinceConverterUtils;
import com.zyuc.iot.etl.util.UserInfoIncrConverterUtils;
import com.zyuc.stat.utils.DateUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.hive.HiveContext;
import scala.Function1;

import java.util.ArrayList;
import java.util.List;

/**
 * @author mengjd
 * @create 2017-10-24 15:43
 **/
public class UserInfoIncrProcess {
    public static void main(String[] args) throws Exception{
        SparkConf conf =new SparkConf().setAppName("userinfo").setMaster("local");
        JavaSparkContext sc=new JavaSparkContext(conf);
        HiveContext hiveContext = new HiveContext(sc);
        hiveContext.sql("use " + ConfigProperties.IOT_HIVE_DATABASE);
        String dataDayid = sc.getConf().get("spark.app.dataDayid", "20171025");
        String userPrePartionD = sc.getConf().get("spark.app.userPrePartionD", "20171024");  // 增量用户关联时候， 需要关联前一天的全量数据

        String syncType = sc.getConf().get("spark.app.syncType", "aincr");

        String inputPath = sc.getConf().get("spark.app.inputPath", "/hadoop/IOT/ANALY_PLATFORM/BasicData/UserInfo");
        String outputPath = sc.getConf().get("spark.app.outputPath", "/hadoop/IOT/data/basic/user/");

        String userOutputPath = outputPath + "/userInfo/data/d=" +  dataDayid;
        String userAndDomainOutputPath = outputPath + "/userAndDomain/data/d=" +  dataDayid;
        String companyAndDomainOutputPath = outputPath + "/companyAndDomain/data/d=" +  dataDayid;

        String userInfoTable = sc.getConf().get("spark.app.userInfoTable", "iot_basic_userinfo");
        String userAndDomainTable = sc.getConf().get("spark.app.userAndDomainTable", "iot_basic_user_and_domain");
        String companyAndDomainTable = sc.getConf().get("spark.app.companyAndDomainTable", "iot_basic_company_and_domain");

        String provinceMapcodeFile = sc.getConf().get("spark.app.provinceMapcodeFile", "/hadoop/IOT/ANALY_PLATFORM/BasicData/iotDimMapcodeProvince/iot_dim_mapcode_province.txt");

        String fileWildcard = sc.getConf().get("spark.app.fileWildcard", "incr_userinfo_qureyes_20171008*" );
        DataFrame df;
        String fileLocation = inputPath + "/" + fileWildcard;
        System.out.println("syncType=========================" + syncType);
        if(syncType.equals("incr")){
            JavaRDD<Row> txtrdd = loadFileToRDD(hiveContext,fileLocation);
            df = savaIncrUserInfo(hiveContext,txtrdd,userInfoTable,dataDayid,userPrePartionD);
            df.write().format("orc").mode(SaveMode.Overwrite).save(userOutputPath);
            hiveContext.sql("alter table "+userInfoTable+" add if not exists partition(d='"+dataDayid+"') ");
            System.out.println("########################incr");
        }else {
            JavaRDD<Row> txtrdd = loadFileToRDD(hiveContext,fileLocation);
            df = hiveContext.createDataFrame(txtrdd,UserInfoIncrConverterUtils.parseType());
            df.repartition(7).write().format("orc").mode(SaveMode.Overwrite).save(userOutputPath);
           hiveContext.sql("alter table " + userInfoTable + " add if not exists partition(d='" + dataDayid+"') ");
            System.out.println("########################all");
        }
        // 用户和企业关联表
        DataFrame tmpDF = df.select("mdn", "imsicdma", "imsilte", "companycode", "vpdndomain", "isvpdn", "isdirect", "userstatus", "atrbprovince", "userprovince", "belo_city", "belo_prov", "custstatus", "custtype", "prodtype","internetType","vpdnOnly","isCommon");
        JavaRDD<Row> usercompRdd = tmpDF.javaRDD().flatMap(new FlatMapFunction<Row, Row>() {
            private static final long serialVersionUID = 1L;
            @Override
            public Iterable<Row> call(Row row) throws Exception {
                List<Row> rows = new ArrayList<>();
                String vpdndomain = row.get(4).toString();
                String[] domainList = vpdndomain.split(",");
                String apn = "-1";
                for(String sub : domainList) {
                    rows.add(UserInfoIncrConverterUtils.parseLine(row,sub,apn));
                }
                return rows;
            }
        });
        DataFrame userCompdf= hiveContext.createDataFrame(usercompRdd, UserInfoIncrConverterUtils.parseUserComparyType());
        userCompdf.printSchema();
        userCompdf.coalesce(7).write().format("orc").mode(SaveMode.Overwrite).save(userAndDomainOutputPath);
        hiveContext.sql("alter table "+userAndDomainTable+" add if not exists partition(d='"+dataDayid+"') ");

        // 企业和域名对应关系表
        String tmpCompanyTable = "tmpCompanyTable";
        userCompdf.select("companycode", "vpdndomain", "belo_prov").distinct().registerTempTable(tmpCompanyTable);

        ProvinceConverterUtils.loadProvinceTable(sc,hiveContext,provinceMapcodeFile);
        ////////////////////////////////////////////////////////////////////////////////////
        // 对域名使用正则过滤： 1. fsznjt.vpdn.gd,,dl.vpdn.hn 清洗为： fsznjt.vpdn.gd,dl.vpdn.hn
        //                  2. ,bdhbgl.vpdn.he 清洗为： bdhbgl.vpdn.he
        ////////////////////////////////////////////////////////////////////////////////////
        DataFrame tmpDomainAndCompanyDF =  hiveContext.sql(
                "select a.companycode, regexp_replace(regexp_replace(vpdndomain,'^,','')," +
                        "',{2}',',') as vpdndomain,b.provincecode,b.provincename from (" +
                        "select companycode, belo_prov, concat_ws(',',collect_set(vpdndomain)) vpdndomain from "+
                        tmpCompanyTable + " group by companycode, belo_prov) a left outer JOIN  (select provincecode,provincename from tmpProvinceTable) b ON a.belo_prov=b.provincecode");
        tmpDomainAndCompanyDF.coalesce(7).write().format("orc").mode(SaveMode.Overwrite).save(companyAndDomainOutputPath);
        hiveContext.sql("alter table "+companyAndDomainTable+" add if not exists partition(d='"+dataDayid+"') ");

        sc.stop();
        System.out.println("=====end");
    }
    public static JavaRDD<Row> loadFileToRDD(HiveContext sc,String fileLocation){
        JavaRDD<Row> textDF = sc.read().format("text").load(fileLocation).javaRDD();  // sc.textFile(fileLocation);
           System.out.println("=====begin:"+DateUtils.getNowTime("yyyyMMddHHmmss"));

        JavaRDD<Row> txtrdd = textDF.map(new Function<Row, Row>() {
            private static final long serialVersionUID = 1L;
            public Row call(Row line)
                    throws Exception {
                Row row = UserInfoIncrConverterUtils.parseLine(line.getString(0));
                return row;
            }
        });
        System.out.println("=====end:"+DateUtils.getNowTime("yyyyMMddHHmmss"));

        txtrdd.filter(new Function<Row, Boolean>() {
            @Override
            public Boolean call(Row row) throws Exception {
                return row.length()!=1;
            }
        });
        return txtrdd;
    }
    public static DataFrame savaIncrUserInfo(HiveContext hiveContext,JavaRDD<Row> txtrdd,String userInfoTable, String dataDayid,String userPrePartionD){
        // 用户表， 是否定向业务， 是否vpdn业务
        String incrUserTable = "incrUserTable_" + dataDayid;
        DataFrame df = hiveContext.createDataFrame(txtrdd,UserInfoIncrConverterUtils.parseType()).repartition(7);
        df.registerTempTable(incrUserTable);
        String preDayUserTable = "preDayUserTable_" + userPrePartionD;
        DataFrame preDayDF = hiveContext.sql("select mdn, imsicdma, imsilte, companycode, vpdndomain, isvpdn, isdirect, userstatus, atrbprovince,"+
                "userprovince, belo_city, belo_prov, custstatus, custtype, prodtype, internetType, vpdnOnly, isCommon from " +
                userInfoTable+" where d='"+userPrePartionD+"'");
        preDayDF.registerTempTable(preDayUserTable);
        String resultSQL = "select  nvl(t.mdn, u.mdn) as mdn," +
                "        if(t.mdn is null, u.imsicdma,t.imsicdma) as imsicdma," +
                "        if(t.mdn is null, u.imsilte,t.imsilte) as imsilte," +
                "        if(t.mdn is null, u.companycode,t.companycode) as companycode," +
                "        if(t.mdn is null, u.vpdndomain,t.vpdndomain) as vpdndomain," +
                "        if(t.mdn is null, u.isvpdn,t.isvpdn) as isvpdn," +
                "        if(t.mdn is null, u.isdirect,t.isdirect) as isdirect," +
                "        if(t.mdn is null, u.userstatus,t.userstatus) as userstatus," +
                "        if(t.mdn is null, u.atrbprovince,t.atrbprovince) as atrbprovince," +
                "        if(t.mdn is null, u.userprovince,t.userprovince) as userprovince," +
                "        if(t.mdn is null, u.belo_city,t.belo_city) as belo_city," +
                "        if(t.mdn is null, u.belo_prov,t.belo_prov) as belo_prov," +
                "        if(t.mdn is null, u.custstatus,t.custstatus) as custstatus," +
                "        if(t.mdn is null, u.custtype,t.custtype) as custtype," +
                "        if(t.mdn is null, u.prodtype,t.prodtype) as prodtype," +
                "        if(t.mdn is null, u.internetType,t.internetType) as internetType," +
                "        if(t.mdn is null, u.vpdnOnly,t.vpdnOnly) as vpdnOnly," +
                "        if(t.mdn is null, u.isCommon,t.isCommon) as isCommon" +
                "        from "+preDayUserTable+" u full outer join  "+incrUserTable+" t" +
                "        on (u.mdn=t.mdn)";
        DataFrame serDF = hiveContext.sql(resultSQL);
       return serDF;
    }
}
