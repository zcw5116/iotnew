package com.zyuc.iot.es.utils;

import java.util.Enumeration;
import java.util.ResourceBundle;

/**
 * ElasticSearchBundle 资源解析
 * @author zhuyuan 2015-10-15 19:16:49
 *
 */
public class ElasticSearchBundle {
	
	//默认配置文件名称
	private static final String BUNDLE_NAME = "elasticsearch";

	private static ResourceBundle bundle = null;

	/**
	 * 初始化配置文件资源
	 * @return
	 */
	public static ResourceBundle getBundle() {
		if (bundle == null) {
			try {
				bundle = ResourceBundle.getBundle(BUNDLE_NAME);
			} catch (Exception e) {
				throw new ExceptionInInitializerError("初始化ElasticSearchBundle失败！");
			}
		}
		return bundle;
	}

	public static String get(String key) {
		return getBundle().getString(key);
	}

	public static Enumeration<String> getKeys() {
		return getBundle().getKeys();
	}

	
	public static void main(String[] args) {
		System.out.println(get("es.device.index"));
	}
}
