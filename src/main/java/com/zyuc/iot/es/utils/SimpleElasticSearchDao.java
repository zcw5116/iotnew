package com.zyuc.iot.es.utils;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;


public abstract class SimpleElasticSearchDao implements ElasticSearchResponse {

	protected Client client = ESClientUtil.getESClient();
	
	protected Client getClient(){
		return client;
	}

	/**
	 * {@inheritDoc}
	 */
	public IndexResponse createIndexResponse(String indexname, String type, String json) {
		IndexResponse response = client.prepareIndex(indexname, type).setSource(json).execute().actionGet();
		return response;
	}

	/**
	 * {@inheritDoc}
	 */
	public IndexResponse createIndexResponse(String indexname, String type, String id, String json) {
		IndexResponse response = client.prepareIndex(indexname, type, id).setSource(json).execute().actionGet();
		return response;
	}

	/**
	 * {@inheritDoc}
	 */
	public GetResponse createGetResponse(String indexname, String type, String id) {
		GetResponse response = client.prepareGet(indexname, type, id).execute().actionGet();
		return response;
	}
	
	/**
	 * 关闭Es客户端
	 */
	protected void closeClient(){
		if(client != null){
			client.close();
		}
	}

}
