package com.zyuc.iot.etl.util;

import com.zyuc.iot.config.ConfigProperties;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

/**
 * @author mengjd
 * @create 2017-10-26 15:04
 **/
public class ProvinceConverterUtils {

    public static void loadProvinceTable(JavaSparkContext sc,HiveContext hiveContext,String provinceMapcodeFile){
        JavaRDD<String> textDF = sc.textFile(provinceMapcodeFile);
        JavaRDD<Row> tmpRDD = textDF.map(new Function<String,Row>(){
            @Override
            public Row call(String line) throws Exception {
                String[] splited = line.split("\t");
                return RowFactory.create(splited[0],splited[1],splited[2],splited[3],splited[4]);
            }
        });
        List<StructField> structFields = new ArrayList<StructField>();
        structFields.add(DataTypes.createStructField("provincecode", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("provincename", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("citycode", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("cityname", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("zipcode", DataTypes.StringType, true));
        StructType structType =DataTypes.createStructType(structFields);
        DataFrame df = hiveContext.createDataFrame(tmpRDD,structType).repartition(1);
        df.registerTempTable("tmpProvinceTable");
    }
    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf().setAppName("province").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        HiveContext hiveContext = new HiveContext(sc);
        hiveContext.sql("use " + ConfigProperties.IOT_HIVE_DATABASE);
        String provinceMapcodeFile = "/hadoop/IOT/ANALY_PLATFORM/BasicData/iotDimMapcodeProvince/iot_dim_mapcode_province.txt";
        loadProvinceTable(sc,hiveContext,provinceMapcodeFile);
        DataFrame result = hiveContext.sql("select * from tmpProvinceTable");
        List<Row> listRow = result.javaRDD().collect();
        for(Row row : listRow){
            System.out.println(row);
        }
    }
}
