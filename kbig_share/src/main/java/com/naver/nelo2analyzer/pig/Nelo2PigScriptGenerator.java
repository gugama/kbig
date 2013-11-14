/**
 * @mainpage nelo2_analyzer Documentation
 * @section 모듈 아키텍쳐
 * @image html architecture.JPG
 * - 이기종 하둡클러스터를 연결하는 목적을 가지고 있습니다.
 * - Nelo2 로그가 저장된 하둡클러스터에서 데이터를 pig를 통해 분석하여 \n
 * nReport의 하둡클러스터로 이관하고, 하이브 테이블에 저장하여 향후 OLAP뷰에서 이용할 수 있도록 지원하는 모듈입니다.
 * @section 사용법
 * - 차후 상술 예정 
 * 
 */
/**
 * @class Nelo2PigScriptGenerator
 * @brief nelo2_analyzer의 핵심 클래스입니다.
 * @file Nelo2PigScriptGenerator.java
 * @brief nelo2_analyzer의 핵심 소스파일입니다.
 */
package com.naver.nelo2analyzer.pig;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.log4j.Logger;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;

import com.naver.nelo2analyzer.udf.Nelo2LogSplit;
import com.naver.nelo2analyzer.udf.TextLoaderPlusTime;
import com.naver.nelo2analyzer.util.TimerUtil;

public class Nelo2PigScriptGenerator {
	private static Properties properties;
	private Date date;
	private SimpleDateFormat formater;
	private String nowDateTime;
	private String resultPath;
	private String pigScriptPath;
	private String input;
	private FileSystem fsPig;
	private FileSystem fsHive;
	private Logger log;

	/**
	 * @author SEUNGJIN LEE
	 * 
	 *         # 외부 응용을 위한 간단한 API guide
	 * 
	 *         # 첫번째 사용법
	 * 
	 *         Nelo2PigScriptGenerator nelo2=new Nelo2PigScriptGenerator();
	 *         nelo2.run(null); - 이 경우 프로젝트 경로내의 test.properties에서 정보를 얻어와서 작업을
	 *         수행한다.
	 * 
	 *         # 두번째 사용법
	 * 
	 *         Nelo2PigScriptGenerator nelo2=new Nelo2PigScriptGenerator();
	 *         nelo2.run(Properties properties); - 이 경우 인자로 받은 properties의 정보를
	 *         바탕으로 작업을 수행한다. - 인자로 담아야 하는 정보들은 test.properties에 주석과 함께 기재되어 있다.
	 */
	public static void main(String args[]) throws Exception {
		PigServer pigServer = null;
		Properties props = new Properties();
		props.setProperty("fs.default.name", "hdfs://10.101.51.221:54310");
		props.setProperty("mapred.job.tracker", "hdfs://10.101.51.221:9001");
		try {
			pigServer = new PigServer(ExecType.MAPREDUCE, props);
			pigServer.registerJar("lib/test1.jar");
//			pigServer.registerJar("lib/test2.jar");
//			pigServer.registerJar("lib/jhannanum.jar");
//			pigServer.registerJar("lib/resource.jar");
//			pigServer.registerJar("lib/nelo2_pig_udf.jar");
			pigServer.registerScript("Kbig.pig");
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				pigServer.shutdown();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * @brief 변수 초기화 로직
	 * @param prop
	 *            .properties파일을 읽기 위한 변수
	 * @remark prop이 없을 경우 실행 경로의 test.properties파일을 읽는다.
	 */
	public void initialize(Properties prop) {
		try {
			// 설정파일을 읽어옴
			// properties.load(new FileInputStream(args[0]));
			new Nelo2LogSplit();
			new TextLoaderPlusTime();
			properties = new Properties();
			if (prop == null) {
				properties.load(new FileInputStream("test.properties"));
			} else
				properties = prop;

			log = Logger.getLogger(this.getClass());
			// 현재시각을 밀리세컨드 단위까지 설정
			date = new Date();
			formater = new SimpleDateFormat("yyyyMMddHHmmss", Locale.KOREA);
			nowDateTime = formater.format(date);

			// 피그분석결과물이 저장될 경로(HDFS)와 임시 피그스크립트가 저장될경로(로컬), 분석대상 데이터가 위치한
			// 경로(HDFS)
			resultPath = "/temporary/" + nowDateTime;
			pigScriptPath = "test.pig";
			input = (String) properties.get("inputPath");

			// pig와 hive가 위치한 하둡클러스터 파일시스템
			fsPig = getHdfsAccess(properties.getProperty("pigHDFS"),
					properties.getProperty("pigMR"));
			fsHive = getHdfsAccess(properties.getProperty("hiveHDFS"),
					properties.getProperty("hiveMR"));
		} catch (Exception e) {
			e.printStackTrace();
			log.warn(TimerUtil.getTime() + "error while initializing");
		}
	}

	/**
	 * 자원 반납 로직
	 */
	public void terminate() {
		try {
			fsPig.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		try {
			fsHive.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		File file = new File(pigScriptPath);
		if (file.isFile()) {
			file.delete();
		}
	}

	/**
	 * @brief 피그스크립트로 Nelo 로그를 분석하고 이기종 하이브 테이블에 삽입하는 로직이다.
	 * @param prop
	 *            .properties파일을 읽기 위한 변수
	 * @remark 외부의 응용에서는 Nelo2PigScriptGenerator.run()을 호출하여 해당 모듈을 수행할 수 있다.\n
	 */
	public void run(Properties prop) {
		try {
			initialize(prop);

			// 분석대상 필드 입력
			String[] targetFields = ((String) properties.get("targetFields"))
					.split(",");

			log.info(TimerUtil.getTime() + "샘플테스트 시작");
			log.info(TimerUtil.getTime() + "분석결과는 /temporary/" + nowDateTime
					+ "에 저장됩니다.");
			log.info(TimerUtil.getTime()
					+ "-------------------------------------------");

			// grep 기능 사용시 필터링할 필드와 밸류값을 설정
			String[] grepFields = ((String) properties.get("grep")).split(",");
			Map<String, String> filterMap = new HashMap<String, String>();
			for (int i = 0; i < grepFields.length; i++) {
				filterMap.put(grepFields[i].split("%")[0],
						grepFields[i].split("%")[1]);
			}

			// 피그 스크립트를 프로그램 실행 폴더내에 'test.pig'로 임시저장, 이후 삭제
			// String script = generatePigScript(targetFields, input);
			String script = generatePigScriptGrep(targetFields, input,
					filterMap);
			// String script = generatePigScriptGrep(targetFields,
			// input,filterMap);

			storePigScript(pigScriptPath, script);
			log.info(TimerUtil.getTime() + script);

			// 저장된 피그스크립트를 실행
			while (true) {
				if (new File(pigScriptPath).length() > 0) {
					executeScript();
					break;
				}
			}
			// readResult(fsPig,resultPath);

			// 분석결과를 콘솔창에서 보고싶다면 아랫줄의 주석을 해제할것

			// 피그스크립트 수행 결과물을 합쳐서 하이브가 위치한 클러스터로 전송, resultPath+"/hive"라는 파일명으로
			// 생성된다.
			combineResult();

			// 하이브로 저장
			HiveConnector hc = new HiveConnector();
			while (true) {
				if (fsHive.getFileStatus(new Path(resultPath + "/hive"))
						.getLen() > 0) {
					// 테이블이 존재하지 않을경우 테이블을 생성 후 삽입
					if (properties.getProperty("tableExists").isEmpty()
							|| properties.getProperty("tableExists").equals(
									"false")) {
						// 테이븖 명이 존재하지 않을경우 현재시간을 이용, 아닐경우 존재하는 테이블명으로 테이블 생성
						if (!properties.getProperty("hiveTableName").isEmpty()) {
							hc.create(properties.getProperty("hiveTableName"),
									targetFields);
						} else {
							hc.create("test" + nowDateTime, targetFields);
						}
						hc.loadData(resultPath + "/hive", "test" + nowDateTime);
						// 테이블이 존재하므로 입시테이블에 자료를 담았다가 존재하는 테이블에 파티션을 생성하여 삽입
					} else {
						log.info(TimerUtil.getTime() + "테이블 존재하므로 인서트쿼리 실행");
						hc.create("test" + nowDateTime, targetFields);
						hc.loadData(resultPath + "/hive", "test" + nowDateTime);
						hc.insertInto(properties.getProperty("hiveTableName"),
								"test" + nowDateTime,
								properties.getProperty("hiveColumnName"));
					}
					break;
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			log.warn(e.getMessage());
		} finally {
			terminate();
		}
	}

	/**
	 * @brief group by 분석의 결과물을 저장시키는 피그스크립트를 생성한다
	 * @param targetFields
	 *            분석대상이 되는 필드명 배열
	 * @param input
	 *            하둡클러스터 상에서 분석대상 파일이 위치한 경로(recursive)
	 * @return 생성된 스크립트를 반환한다
	 */
	public String generatePigScript(String[] targetFields, String input) {
		StringBuilder pigScript = new StringBuilder();
		pigScript.append("SET mapred.child.java.opts -Xmx4096m;\n");
		pigScript.append("SET io.sort.mb 200;\n");
		pigScript.append("SET io.sort.factor 100;\n");
		pigScript.append("SET io.file.buffer.size 131072;\n");
		pigScript.append("SET pig.maxCombinedSplitSize 1073741824;\n");
		pigScript.append("a = load '" + input
				+ "' using TextLoader() as line:chararray;\n");
		// pigScript.append("a = load '" + input
		// + "' using com.naver.nelo2analyzer.udf.TextLoaderPlusTime('"
		// + properties.getProperty("dateIndex")
		// + "') as line:chararray;\n");
		pigScript
				.append("b = foreach a generate com.naver.nelo2analyzer.udf.Nelo2LogSplit($0,'\\t',':',");
		for (int i = 0; i < targetFields.length; i++) {
			pigScript.append("'" + targetFields[i] + "',");
		}
		pigScript.deleteCharAt(pigScript.length() - 1);
		pigScript.append(");\n");
		pigScript.append("c = group b by $0;\n");
		pigScript.append("d = foreach c generate flatten(group), COUNT(b);\n");
		pigScript.append("store d into '" + resultPath + "';");
		return pigScript.toString();
	}

	/**
	 * @brief group by와 grep 분석의 결과물을 저장시키는 피그스크립트를 생성한다
	 * @param targetFields
	 *            분석대상이 되는 필드명 배열
	 * @param input
	 *            하둡클러스터 상에서 분석대상 파일이 위치한 경로(recursive)
	 * @param filterMap
	 *            grep분석을 포함할 필드와 대상문자열로 이루어진 해쉬맵\n 예 : (projectName,blog*)
	 * @return
	 */
	public String generatePigScriptGrep(String[] targetFields, String input,
			Map<String, String> filterMap) {
		StringBuilder pigScript = new StringBuilder();
		pigScript.append("SET mapred.child.java.opts -Xmx4096m;\n");
		pigScript.append("SET io.sort.mb 200;\n");
		pigScript.append("SET io.sort.factor 100;\n");
		pigScript.append("SET io.file.buffer.size 131072;\n");
		pigScript.append("SET pig.maxCombinedSplitSize 1073741824;\n");
		pigScript.append("a = load '" + input
				+ "' using TextLoader() as line:chararray;\n");
		pigScript
				.append("b = foreach a generate com.naver.nelo2analyzer.udf.Nelo2LogSplit($0,'\\t',':',");
		for (int i = 0; i < targetFields.length; i++) {
			pigScript.append("'" + targetFields[i] + "',");
		}
		pigScript.deleteCharAt(pigScript.length() - 1);
		pigScript.append(") as line:tuple(");
		for (int i = 0; i < targetFields.length; i++) {
			pigScript.append(targetFields[i] + ":chararray,");
		}
		pigScript.deleteCharAt(pigScript.length() - 1);
		pigScript.append(");\n");
		Iterator<String> keyIterator = filterMap.keySet().iterator();
		pigScript.append("c = filter b by ");
		while (keyIterator.hasNext()) {
			String key = keyIterator.next();
			String value = filterMap.get(key); //
			pigScript.append("line." + key + " matches '" + value + "' and ");
		}
		pigScript.delete(pigScript.length() - 5, pigScript.length());
		pigScript.append(";\n");
		pigScript.append("d = group c by $0;\n");
		pigScript.append("e = foreach d generate flatten(group), COUNT(c);\n");
		pigScript.append("store e into '" + resultPath + "';");
		return pigScript.toString();
	}

	/**
	 * @brief 생성된 피그스크립트를 임시경로에 저장시킨다.
	 * @param path
	 *            피그스크립트를 저장할 경로
	 * @param script
	 *            생성된 피그 스크립트
	 * @return
	 */
	public boolean storePigScript(String path, String script) {
		File file = null;
		FileWriter fw = null;
		BufferedWriter bw = null;
		PrintWriter pw = null;
		try {
			file = new File(path);
			if (file.isFile()) {
				file.delete();
				file = new File(path);
			}
			if (!file.exists())
				file.createNewFile();
			fw = new FileWriter(file);
			bw = new BufferedWriter(fw);
			pw = new PrintWriter(bw, true);
			pw.println(script);
			file.createNewFile();
		} catch (Exception e) {
			e.printStackTrace();
			log.warn(e.getMessage());
			return false;
		} finally {
			if (fw != null) {
				try {
					fw.close();
				} catch (IOException e) {
					e.printStackTrace();
					log.warn(e.getMessage());
				}
			}
			if (bw != null) {
				try {
					bw.close();
				} catch (IOException e) {
					e.printStackTrace();
					log.warn(e.getMessage());
				}
			}
			if (pw != null) {
				pw.close();
			}
		}
		return true;
	}

	/**
	 * @brief 네임노드와 잡트래커의 주소를 입력받아 하둡클러스터에 접근하고, 그 파일시스템을 반환한다.
	 * @param nameNode
	 *            nameNode의 url
	 * @param jobTracker
	 *            jobTracker의 url
	 * @return
	 */
	public FileSystem getHdfsAccess(String nameNode, String jobTracker) {
		FileSystem hdfs = null;
		try {
			Configuration conf = new Configuration();
			conf.set("fs.default.name", nameNode);
			conf.set("mapred.job.tracker", jobTracker);
			hdfs = FileSystem.get(conf);
		} catch (IOException e) {
			e.printStackTrace();
			log.warn(e.getMessage());
		}
		return hdfs;
	}

	/**
	 * @brief 피그분석의 결과물을 콘솔에 출력시키는 메소드
	 * @param hdfs
	 *            피그스크립트가 수행되는 하둡클러스터
	 * @param path
	 *            피그 분석의 결과물이 저장된 경로
	 */
	public void readResult(FileSystem hdfs, String path) {
		FileStatus[] fs;
		FSDataInputStream is = null;
		String linePath;
		try {
			fs = hdfs.listStatus(new Path(path));
			for (int i = 0; i < fs.length; i++) {
				if (!fs[i].getPath().toString().contains("part"))
					continue;
				is = hdfs.open(fs[i].getPath());
				while ((linePath = is.readLine()) != null) {
					log.info(TimerUtil.getTime() + linePath);
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
			log.warn(e.getMessage());
		}
	}

	/**
	 * @brief 피그스크립트 수행 결과물을 합쳐서 하이브가 위치한 클러스터로 전송, resultPath+"/hive"라는 파일명으로
	 *        생성된다.
	 */
	public void combineResult() {
		FileStatus[] fs;
		FSDataInputStream is = null;
		FSDataOutputStream out = null;

		try {
			out = fsHive.create(new Path(resultPath + "/hive"));
			fs = fsPig.listStatus(new Path(resultPath));
			for (int i = 0; i < fs.length; i++) {
				if (!fs[i].getPath().toString().contains("part"))
					continue;
				is = fsPig.open(fs[i].getPath());
				log.info(TimerUtil.getTime() + "fs path : " + fs[i].getPath());
				IOUtils.copyBytes(is, out, 4096, false);
			}
		} catch (IOException e) {
			e.printStackTrace();
			log.warn(e.getMessage());
		} finally {
			if (out != null) {
				try {
					out.close();
				} catch (IOException e) {
					e.printStackTrace();
					log.warn(e.getMessage());
				}
			}
		}
	}

	/**
	 * @brief 해당 스크립트를 실행시키는 메소드 빌드패스 상에 nelo2analyzer.jar를 가지고있어야 한다.\n udf를
	 *        프로젝트상의 클래스파일에서 읽어오는 것이 아니라 등록된 jar파일에서 읽어오는 식으로 PigServer가 동작하기
	 *        때문이다.
	 */
	public boolean executeScript() {
		PigServer pigServer = null;
		Properties props = new Properties();
		props.setProperty("fs.default.name", properties.getProperty("pigHDFS"));
		props.setProperty("mapred.job.tracker", properties.getProperty("pigMR"));
		try {
			pigServer = new PigServer(ExecType.MAPREDUCE, props);
			pigServer.registerJar("nelo2_pig_udf.jar");
			pigServer.registerScript(pigScriptPath);
		} catch (Exception e) {
			e.printStackTrace();
			log.warn(e.getMessage());
			return false;
		} finally {
			try {
				pigServer.shutdown();
			} catch (Exception e) {
				e.printStackTrace();
				log.warn(e.getMessage());
			}
		}
		return true;
	}

	/**
	 * @brief 외부응용에서 설정을 획득하기 위한 메소드
	 * @return
	 */
	public Properties getProperties() {
		return properties;
	}
}
