package com.zyuc.iot.stat.hbase;

import com.zyuc.stat.properties.ConfigProperties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.sql.*;

/**
 * Created by zhoucw on 17-10-20.
 */
public class HbaseData2Oracle {
    static Configuration conf = null;
    static String driverUrl = "";
    static String dbUser = "";
    static String dbPasswd = "";

    static {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.property.clientPort", ConfigProperties.IOT_ZOOKEEPER_CLIENTPORT());
        conf.set("hbase.zookeeper.quorum", ConfigProperties.IOT_ZOOKEEPER_QUORUM());
        driverUrl = "jdbc:oracle:thin:@100.66.124.129:1521/dbnms";
        dbUser  = "epcslview";
        dbPasswd = "epc_slview129";
    }


    public static Connection getConnection(){
        Connection conn = null;
        try {
            conn = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return conn;
    }

    public static void getResultScann(String tableName, String start_rowkey,
                                      String stop_rowkey, String statDate) throws IOException, SQLException, ClassNotFoundException {

        String monthid = statDate;
        String deleteSQL = "delete from  iot_stat_company_usernum where STATTIME='" + monthid + "' ";
        java.sql.Connection dbConn = DriverManager.getConnection(driverUrl, dbUser, dbPasswd);
        PreparedStatement psmt = null;
        psmt = dbConn.prepareStatement(deleteSQL);
        psmt.executeUpdate();
        psmt.close();

        Class.forName("oracle.jdbc.driver.OracleDriver");

        Connection conn = getConnection();

        Scan scan = new Scan();
        scan.setStartRow(Bytes.toBytes(start_rowkey));
        scan.setStopRow(Bytes.toBytes(stop_rowkey));
        TableName tName = TableName.valueOf(tableName);
        Table table = conn.getTable(tName);
        String sql = "INSERT INTO iot_stat_company_Usernum (STATTIME, COMPANYCODE, TOTALNUM, DNUM, VNUM, DRANK, VRANK, CYCLETYPE) VALUES (?,?,?,?,?,?,?,?)";
        ResultScanner rs = null;
        try {
            rs = table.getScanner(scan);
            java.sql.Connection dbConn1 = DriverManager.getConnection(driverUrl, dbUser, dbPasswd);
            dbConn1.setAutoCommit(false);
            PreparedStatement pstmt= (PreparedStatement) dbConn1.prepareStatement(sql);
            int i = 0;
            for (Result r : rs) {

                String row = new String(r.getRow());
                String code = new String(r.getValue(Bytes.toBytes("u"), Bytes.toBytes("ccode")));
                String tsum = new String(r.getValue(Bytes.toBytes("u"), Bytes.toBytes("tsum")));
                String dsum = new String(r.getValue(Bytes.toBytes("u"), Bytes.toBytes("dsum")));
                String vsum = new String(r.getValue(Bytes.toBytes("u"), Bytes.toBytes("vsum")));
                String drank = new String(r.getValue(Bytes.toBytes("u"), Bytes.toBytes("drank")));
                String vrank =new String(r.getValue(Bytes.toBytes("u"), Bytes.toBytes("vrank")));

                pstmt.setString(1, statDate);
                pstmt.setString(2, code);
                pstmt.setString(3, tsum);
                pstmt.setString(4, dsum);
                pstmt.setString(5, vsum);
                pstmt.setString(6, drank);
                pstmt.setString(7, vrank);
                pstmt.setString(8, "M");
                i++;
                pstmt.addBatch();
                if(i%1000==0){
                    pstmt.executeBatch();
                }
                System.out.println("row:" + row + " code:" + code + " tsum:" + tsum + " dsum:" + dsum + " vsum:" + vsum + " drank:" + drank + " vrank:" + vrank);
            }

            pstmt.executeBatch();
            dbConn1.commit();
            pstmt.close();
            dbConn1.close();

        } finally {
            rs.close();
            conn.close();
        }
    }

    public static void main(String[] args) {
        if(args.length<1){
            System.out.println("参数错误 ");
            System.exit(-1);
        }

        String monthid = args[0];
        String startRowkey = monthid + "_-";
        String endRowkey = monthid + "_P999999999";

        try {
            getResultScann("iot_stat_company_usernum", startRowkey, endRowkey, monthid);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
