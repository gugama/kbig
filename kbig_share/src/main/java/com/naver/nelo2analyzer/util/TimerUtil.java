package com.naver.nelo2analyzer.util;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @class TimerUtil
 * @brief 시간에 관련된 유틸리티입니다.
 * @file TimerUtil.java
 * @brief 시간에 관련된 유틸리티입니다.
 */
public class TimerUtil {
	public static void main(String[] args) {
		fileCopy();
	}

	public static String getTime() {
		SimpleDateFormat format = new SimpleDateFormat("[yyyy-mm-dd hh:mm:ss]");
		String currTime = format.format(new Date());
		return currTime;
	}

	public static void fileCopy() {
		DataInputStream dis = null;
		DataOutputStream dos = null;
		String linePath = null;
		File dName = new File("c:/algo_hw/hw3");
		try {
			dos = new DataOutputStream(new FileOutputStream(
					"c:/algo_hw/hw3/input2.txt"));
			File[] files = dName.listFiles();
			for (File file : files) {
				if (!file.getName().contains("part"))
					continue;
				dis = new DataInputStream(new FileInputStream(file));
				while ((linePath = dis.readLine()) != null) {
					dos.writeChars(linePath + '\n');
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
