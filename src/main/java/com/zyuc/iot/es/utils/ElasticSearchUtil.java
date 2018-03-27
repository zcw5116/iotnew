package com.zyuc.iot.es.utils;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

public class ElasticSearchUtil {
	
	
	public static String generateJson(Object object){
		ObjectMapper mapper = new ObjectMapper();
		try {
			return new String(mapper.writeValueAsBytes(object), "UTF-8");
		} catch (JsonGenerationException e) {
			e.printStackTrace();
		} catch (JsonMappingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	
	public static void main(String[] args) {
		Map<String, String> alarm = new HashMap<String, String>();
		alarm.put("id", "123456");
		alarm.put("alarmname", "朱元");
		alarm.put("alarmtype", "zhongduus");
		alarm.put("alarmlevel", "1");
		
		System.out.println(generateJson(alarm));
		
	}
}
