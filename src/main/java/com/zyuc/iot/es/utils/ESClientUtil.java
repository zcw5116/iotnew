package com.zyuc.iot.es.utils;

import org.apache.log4j.Logger;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;

import java.net.InetAddress;

/**
 * ES 客户端工具类，创建、关闭ES客户端,
 * 
 * @author zhuyuan
 *
 */
public class ESClientUtil {
    private static Logger logger = Logger.getLogger(ESClientUtil.class);

    private static String esClusterName; // ES集群名称
    private static String esIP; // 访问ES服务器IP地址
    private static Integer esPort; // 访问ES端口号

    static {
        try {
            esClusterName = ElasticSearchBundle.get("es.cluster.name");
            esIP = ElasticSearchBundle.get("es.ip");
            esPort = Integer.parseInt(ElasticSearchBundle.get("es.port"));
            logger.info("ESClusterName = " + esClusterName + ", ESIP = " + esIP + ", ESPort = " + esPort);
        } catch (Exception e) {
            logger.error("Initial paras error: " + e.getMessage());
        }
    }
    
    

    public static String getEsClusterName() {
        if(esClusterName == null){
            esClusterName = ElasticSearchBundle.get("es.cluster.name");
        }
        return esClusterName;
    }

    public static String getEsIP() {
        if(esIP == null){
            esIP = ElasticSearchBundle.get("es.ip");
        }
        return esIP;
    }

    public static Integer getEsPort() {
        if(esPort == null){
            esPort = Integer.parseInt(ElasticSearchBundle.get("es.port"));
        }
        return esPort;
    }

    // 初始化配置信息
    private static Settings settings = Settings.settingsBuilder()
            // 设置集群名称
            .put("cluster.name", getEsClusterName())
            // 探测集群中机器状态
            .put("client.transport.sniff", false).build();

    /**
     * 获取ES客户端
     * 
     * @return
     */
    @SuppressWarnings("resource")
    public static Client getESClient() {
        Client client = null;
        try {
            InetAddress addr1 = InetAddress.getByName(getEsIP());
            TransportAddress transportAddress = new InetSocketTransportAddress(addr1, getEsPort());
            client = TransportClient.builder().settings(settings).build().addTransportAddress(transportAddress);
            logger.info("aaaaaaaaaaaaaaa: " + client.admin().cluster().prepareState().get().getClusterName());
        } catch (Exception e) {
            client.close();
            logger.error("获取ES客户端失败！", e); 
        }
        return client;
    }

    /**
     * 关闭ES客户端
     * 
     * @param client
     */
    public static void closeESClient(Client client) {
        if (client != null) {
            client.close();
        }
    }

    public static void main(String[] args) {

        System.out.println(getESClient());
    }
}
