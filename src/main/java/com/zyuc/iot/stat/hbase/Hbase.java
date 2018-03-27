package com.zyuc.iot.stat.hbase;

/**
 * Created by zhoucw on 17-10-19.
 */
import java.io.IOException;
import java.util.Arrays;

import com.zyuc.stat.properties.ConfigProperties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

public class Hbase {
    // 声明静态配置
    static Configuration conf = null;
    static {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.property.clientPort", ConfigProperties.IOT_ZOOKEEPER_CLIENTPORT());
        conf.set("hbase.zookeeper.quorum", ConfigProperties.IOT_ZOOKEEPER_QUORUM());
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
    /*
     * 创建表
     *
     * @tableName 表名
     *
     * @family 列族列表
     */
    public static void creatTable(String tableName, String[] family)
            throws IOException {

        Connection conn = getConnection();
        Admin admin = conn.getAdmin();
        TableName tName = TableName.valueOf(tableName);

        HTableDescriptor tableDesc = new HTableDescriptor(tName);

        for (int i = 0; i < family.length; i++) {
            tableDesc.addFamily(new HColumnDescriptor(family[i]));
        }
        if (admin.tableExists(tName)) {
           // admin.disableTable(tName);
           // admin.deleteTable(tName);
            System.out.println("table Exists!");
            System.exit(0);
        } else {
            admin.createTable(tableDesc);
            System.out.println("create table Success!");
        }
    }

    public static void addRow(String tableName, String row,
                              String columnFamily, String column, String value) throws IOException {

        Connection conn = getConnection();
        TableName tName = TableName.valueOf(tableName);
        Table table = conn.getTable(tName);

        Put put = new Put(Bytes.toBytes(row));// 设置rowkey
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(value));

        table.put(put);
    }

    /*
     * 根据rwokey查询
     *
     * @rowKey rowKey
     *
     * @tableName 表名
     */
    public static Result getResult(String tableName, String rowKey)
            throws IOException {
        Get get = new Get(Bytes.toBytes(rowKey));
        HTable table = new HTable(conf, Bytes.toBytes(tableName));// 获取表
        Result result = table.get(get);
        for (KeyValue kv : result.list()) {
            System.out.println("family:" + Bytes.toString(kv.getFamily()));
            System.out
                    .println("qualifier:" + Bytes.toString(kv.getQualifier()));
            System.out.println("value:" + Bytes.toString(kv.getValue()));
            System.out.println("Timestamp:" + kv.getTimestamp());
            System.out.println("-------------------------------------------");
        }
        return result;
    }


    /*
     * 遍历查询hbase表
     *
     * @tableName 表名
     */
    public static void getResultScann(String tableName, String start_rowkey,
                                      String stop_rowkey) throws IOException {

        Connection conn = getConnection();

        Scan scan = new Scan();
        scan.setStartRow(Bytes.toBytes(start_rowkey));
        scan.setStopRow(Bytes.toBytes(stop_rowkey));
        TableName tName = TableName.valueOf(tableName);
        Table table = conn.getTable(tName);

        ResultScanner rs = null;
        try {
            rs = table.getScanner(scan);
            for (Result r : rs) {
                for (Cell rowKV : r.rawCells()) {
                    System.out.print("Row Name: " + new String(CellUtil.cloneRow(rowKV)) + " ");
                    System.out.print("Timestamp: " + rowKV.getTimestamp() + " ");
                    System.out.print("column Family: " + new String(CellUtil.cloneFamily(rowKV)) + " ");
                    System.out.print("column Name:  " + new String(CellUtil.cloneQualifier(rowKV)) + " ");
                    System.out.println("Value: " + new String(CellUtil.cloneValue(rowKV)) + " ");
                }
            }
        } finally {
            rs.close();
            conn.close();
        }
    }

    /*
     * 查询表中的某一列
     *
     * @tableName 表名
     *
     * @rowKey rowKey
     */
    public static void getResultByColumn(String tableName, String rowKey,
                                         String familyName, String columnName) throws IOException {
        HTable table = new HTable(conf, Bytes.toBytes(tableName));
        Get get = new Get(Bytes.toBytes(rowKey));
        get.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(columnName)); // 获取指定列族和列修饰符对应的列
        Result result = table.get(get);
        for (KeyValue kv : result.list()) {
            System.out.println("family:" + Bytes.toString(kv.getFamily()));
            System.out
                    .println("qualifier:" + Bytes.toString(kv.getQualifier()));
            System.out.println("value:" + Bytes.toString(kv.getValue()));
            System.out.println("Timestamp:" + kv.getTimestamp());
            System.out.println("-------------------------------------------");
        }
    }

    /*
     * 更新表中的某一列
     *
     * @tableName 表名
     *
     * @rowKey rowKey
     *
     * @familyName 列族名
     *
     * @columnName 列名
     *
     * @value 更新后的值
     */
    public static void updateTable(String tableName, String rowKey,
                                   String familyName, String columnName, String value)
            throws IOException {
        HTable table = new HTable(conf, Bytes.toBytes(tableName));
        Put put = new Put(Bytes.toBytes(rowKey));
        put.add(Bytes.toBytes(familyName), Bytes.toBytes(columnName),
                Bytes.toBytes(value));
        table.put(put);
        System.out.println("update table Success!");
    }

    /*
     * 查询某列数据的多个版本
     *
     * @tableName 表名
     *
     * @rowKey rowKey
     *
     * @familyName 列族名
     *
     * @columnName 列名
     */
    public static void getResultByVersion(String tableName, String rowKey,
                                          String familyName, String columnName) throws IOException {
        HTable table = new HTable(conf, Bytes.toBytes(tableName));
        Get get = new Get(Bytes.toBytes(rowKey));
        get.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(columnName));
        get.setMaxVersions(5);
        Result result = table.get(get);
        for (KeyValue kv : result.list()) {
            System.out.println("family:" + Bytes.toString(kv.getFamily()));
            System.out
                    .println("qualifier:" + Bytes.toString(kv.getQualifier()));
            System.out.println("value:" + Bytes.toString(kv.getValue()));
            System.out.println("Timestamp:" + kv.getTimestamp());
            System.out.println("-------------------------------------------");
        }
        /*
         * List<?> results = table.get(get).list(); Iterator<?> it =
         * results.iterator(); while (it.hasNext()) {
         * System.out.println(it.next().toString()); }
         */
    }

    /*
     * 删除指定的列
     *
     * @tableName 表名
     *
     * @rowKey rowKey
     *
     * @familyName 列族名
     *
     * @columnName 列名
     */
    public static void deleteColumn(String tableName, String rowKey,
                                    String falilyName, String columnName) throws IOException {
        HTable table = new HTable(conf, Bytes.toBytes(tableName));
        Delete deleteColumn = new Delete(Bytes.toBytes(rowKey));
        deleteColumn.deleteColumns(Bytes.toBytes(falilyName),
                Bytes.toBytes(columnName));
        table.delete(deleteColumn);
        System.out.println(falilyName + ":" + columnName + "is deleted!");
    }

    /*
     * 删除指定的列
     *
     * @tableName 表名
     *
     * @rowKey rowKey
     */
    public static void deleteAllColumn(String tableName, String rowKey)
            throws IOException {
        HTable table = new HTable(conf, Bytes.toBytes(tableName));
        Delete deleteAll = new Delete(Bytes.toBytes(rowKey));
        table.delete(deleteAll);
        System.out.println("all columns are deleted!");
    }

    /*
     * 删除表
     *
     * @tableName 表名
     */
    public static void deleteTable(String tableName) throws IOException {
        HBaseAdmin admin = new HBaseAdmin(conf);
        admin.disableTable(tableName);
        admin.deleteTable(tableName);
        System.out.println(tableName + "is deleted!");
    }

    public static void main(String[] args) throws IOException {


        System.out.println(Arrays.toString(Bytes.toBytes("abc")));
        System.out.println(Arrays.toString("abc".getBytes()));

        getResultScann("analyze_bp_tab", "a", "z");
 /*       // 创建表
        String tableName = "blog2";
        String[] family = { "article", "author" };
        // creatTable(tableName, family);

        // 为表添加数据

        String[] column1 = { "title", "content", "tag" };
        String[] value1 = {
                "Head First HBase",
                "HBase is the Hadoop database. Use it when you need random, realtime read/write access to your Big Data.",
                "Hadoop,HBase,NoSQL" };
        String[] column2 = { "name", "nickname" };
        String[] value2 = { "nicholas", "lee" };
        addData("rowkey1", "blog2", column1, value1, column2, value2);
        addData("rowkey2", "blog2", column1, value1, column2, value2);
        addData("rowkey3", "blog2", column1, value1, column2, value2);

        // 遍历查询
        getResultScann("blog2", "rowkey4", "rowkey5");
        // 根据row key范围遍历查询
        getResultScann("blog2", "rowkey4", "rowkey5");

        // 查询
        getResult("blog2", "rowkey1");

        // 查询某一列的值
        getResultByColumn("blog2", "rowkey1", "author", "name");

        // 更新列
        updateTable("blog2", "rowkey1", "author", "name", "bin");

        // 查询某一列的值
        getResultByColumn("blog2", "rowkey1", "author", "name");

        // 查询某列的多版本
        getResultByVersion("blog2", "rowkey1", "author", "name");

        // 删除一列
        deleteColumn("blog2", "rowkey1", "author", "nickname");

        // 删除所有列
        deleteAllColumn("blog2", "rowkey1");

        // 删除表
        deleteTable("blog2");*/

    }
}