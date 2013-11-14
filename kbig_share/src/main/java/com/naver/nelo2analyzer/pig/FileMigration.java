package com.naver.nelo2analyzer.pig;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

/**
 * @class FileMigration
 * @brief 테스트 데이터를 생성하기 위해 사용되었던 클래스입니다.
 * @file FileMigration.java
 * @brief 테스트 데이터를 생성하기 위해 사용되었던 클래스입니다.
 */
public class FileMigration {
	static int A, B, C, D, E, F = 0;

	public static void main(String[] args) throws Exception {
		fileCopy();
	}

	public static void fileSplit() throws Exception {
		BufferedReader br = new BufferedReader(new FileReader(
				"C:/Users/nhn/Downloads/log_of_19gb.txt"));
		int num = 1;
		String str;
		while ((str = br.readLine()) != null) {
			String path = "C:/data/10kb/" + num + ".txt";
			File file = new File(path);
			if (file.length() > 10240) {
				num += 1;
				System.out.println(num);
				path = "C:/data/10kb/" + num + ".txt";
			}
			BufferedWriter bw = new BufferedWriter(new FileWriter(path, true));
			bw.write(str + "\n");
			bw.close();
		}
	}

	public static void fileDistribution(Path path) throws Exception {
		Configuration conf = new Configuration();
//		conf.set("fs.default.name", "hdfs://10.32.167.196:9000");
//		conf.set("mapred.job.tracker", "hdfs://10.32.167.196:9001");
		FileSystem fs = FileSystem.get(conf);
		FileStatus[] fileStatus = fs.listStatus(path);
		for (FileStatus temp : fileStatus) {
			if (temp.isDir()) {
				fileDistribution(temp.getPath());
			} else {
				long fileSize = temp.getLen();
				BufferedWriter br = new BufferedWriter(new FileWriter(
						"C:/data/dist.txt", true));
				System.out.println(temp.getPath());
				br.write(temp.getPath()+"\t"+fileSize + "\n");
				br.close();
				if (fileSize <= 1024) {
					A += 1;
					// 1KB 초과 10KB?�하
				} else if (fileSize <= 10240) {
					B += 1;
					// 10KB 초과 100KB?�하
				} else if (fileSize <= 102400) {
					C += 1;
					// 100KB 초과 1MB?�하
				} else if (fileSize <= 1048576) {
					D += 1;
					// 1MB 초과 10MB?�하
				} else if (fileSize <= 10485760) {
					E += 1;
					// 10MB?�상
				} else {
					F += 1;
				}
			}
		}
	}

	public static void recursive(String path) throws Exception{
	}
	public static void fileCopy() throws Exception {
		// File directory = new File("C:/data/100mb/");
		// File[] files=directory.listFiles();
		Configuration conf = new Configuration();
		conf.set("fs.default.name", "hdfs://10.101.51.221:54310");
		conf.set("mapred.job.tracker", "hdfs://10.101.51.221:9001");
		FileSystem fs = FileSystem.get(conf);
		
		for(int i=2;i<=9;i++){
			fs.copyFromLocalFile(new Path("C:/Users/nhn/Desktop/2.TwitterData/part-r-0000"+String.valueOf(i)), 
					  new Path("/temporary/kbig/part-r-0000"+String.valueOf(i)));
			fs.close();
		}
		/*
		InputStream in = null;
		FSDataOutputStream out = null;
		File file= new File("C:/Users/nhn/Downloads/JHanNanum-0.8.4-ko/JHanNanum/conf");
		File[] files= file.listFiles();
		
		for(File f:files){
			System.out.println(f.getAbsolutePath());			
		}
		
		if ((new File("C:/NHNLOCL1504.log").exists())) {
			// InputStream in=new BufferedInputStream(new FileInputStream(new
			// File("C:/data/10kb/"+num+".txt")));
			in = new BufferedInputStream(new FileInputStream(new File(
					"C:/NHNLOCL1504.log")));
			out = fs.create(new Path("/test/error/20130920/NHNLOCL1504.log"));
			IOUtils.copyBytes(in, out, 4096, true);
		}
		in.close();
		out.close();
		fs.close();
		// for(File temp:files){
		// System.out.println(temp.getName());
		// InputStream in=new BufferedInputStream(new FileInputStream(temp));
		// OutputStream outStream=fsReal.create(new
		// Path("/test/log_of_650mb.log"));
		// FSDataOutputStream out=fs.create(new
		// Path("/test/100mb/"+temp.getName()));
		// IOUtils.copyBytes(in, out, 4096, true);
		// }
		 
		 */
	}

	public static void removeFolder() throws Exception {
		Configuration conf = new Configuration();
		conf.set("fs.default.name", "hdfs://10.101.51.221:54310");
		conf.set("mapred.job.tracker", "hdfs://10.101.51.221:9001");
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path("/temporary/kbig"), true);
	}

}
