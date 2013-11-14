package com.naver.nelo2analyzer.util;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

/**
 * @class PropertiesUtil
 * @brief Properties 객체 관련 유틸리티입니다.
 * @file PropertiesUtil.java
 * @brief Properties 객체 관련 유틸리티입니다.
 */
public class PropertiesUtil {
	public static Properties loadProperties(Properties properties, String path)
			throws FileNotFoundException, IOException {
		properties = new Properties();
		properties.load(new FileInputStream(path));
		return properties;
	}
}
