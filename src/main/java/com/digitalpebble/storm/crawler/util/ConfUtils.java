package com.digitalpebble.storm.crawler.util;

import java.util.Map;

import backtype.storm.utils.Utils;

/** TODO replace by calls to backtype.storm.utils.Utils **/

public class ConfUtils {

	public static int getInt(Map<String, Object> conf, String key,
			int defaultValue) {
		Object obj = Utils.get(conf, key, defaultValue);
		return Utils.getInt(obj);
	}

	public static long getLong(Map<String, Object> conf, String key,
			long defaultValue) {
		return (Long) Utils.get(conf, key, defaultValue);
	}

	public static float getFloat(Map<String, Object> conf, String key,
			float defaultValue) {
		Object obj = Utils.get(conf, key, defaultValue);
		if (obj instanceof Double)
			return ((Double) obj).floatValue();
		return (Float) obj;
	}

	public static boolean getBoolean(Map<String, Object> conf, String key,
			boolean defaultValue) {
		Object obj = Utils.get(conf, key, defaultValue);
		return Utils.getBoolean(obj, defaultValue);
	}

	public static String getString(Map<String, Object> conf, String key) {
		return (String) Utils.get(conf, key, null);
	}

	public static String getString(Map<String, Object> conf, String key,
			String defaultValue) {
		return (String) Utils.get(conf, key, defaultValue);
	}
	
}
