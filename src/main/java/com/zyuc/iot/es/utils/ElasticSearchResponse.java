package com.zyuc.iot.es.utils;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;

public interface ElasticSearchResponse {
	/**
     * 创建索引
     * @return
     */
    IndexResponse createIndexResponse(String indexname, String type, String json);
    
    /**
     * 创建索引
     * @return
     */
    IndexResponse createIndexResponse(String indexname, String type, String id, String json);
    
    /**
     * 获取索引
     * @return
     */
    GetResponse createGetResponse(String indexname, String type, String id);
    
    
    
}
