package com.naver.nelo2analyzer.pig;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.apache.log4j.Logger;

import com.naver.nelo2analyzer.util.TimerUtil;

/**
 * @class HiveConnector
 * @brief 하이브와 연동하는 드라이버 클래스입니다.
 * @file HiveConnector.java
 * @brief 하이브와 연동하는 드라이버 클래스입니다.
 */
public class HiveConnector {
	// 테스트용
	public static void main(String[] args) throws Exception {
		properties = new Properties();
		properties.setProperty("hiveServer",args[0]);
		Connection con = getConnection();
	}

	private static String driverName = "org.apache.hadoop.hive.jdbc.HiveDriver";
	private static Properties properties;
	private static Logger log;

	public HiveConnector() {
		Nelo2PigScriptGenerator nelo2 = new Nelo2PigScriptGenerator();
		properties = nelo2.getProperties();
		log=Logger.getLogger(this.getClass());
	}

	/**
	 * @brief 커넥션을 얻는 메소드
	 * @return
	 * @throws SQLException
	 */
	public static Connection getConnection() throws SQLException {
		try {
			Class.forName(driverName);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			log.warn(e.getMessage());
			System.exit(1);
		}
		log.info(TimerUtil.getTime() + "커넥션 획득 시도.");
		Connection con = DriverManager.getConnection(
				properties.getProperty("hiveServer"),
				"hive",
				"");
		log.info(TimerUtil.getTime() + "커넥션 획득 성공.");
		return con;
	}

	/**
	 * @brief 파일경로와 테이블명을 인자로 받아 해당 파일을 임시테이블에 로드하는 메소드
	 * @param filePath
	 *            테이블에 삽입할 파일이 위치한 경로
	 * @param tableName
	 *            하이브 테이블 명
	 * @throws SQLException
	 */
	public void loadData(String filePath, String tableName) throws SQLException {
		log.info(TimerUtil.getTime() + "인서트 시작");
		Connection con = null;
		Statement stmt = null;
		try {
			con = getConnection();
			stmt = con.createStatement();
			String sql = "load data inpath '" + filePath + "' into table "
					+ tableName;
			log.info(TimerUtil.getTime() + sql);
			stmt.execute(sql);
			log.info(TimerUtil.getTime() + "load data 성공");
		} catch (Exception e) {
			e.printStackTrace();
			log.warn(e.getMessage());
		} finally {
			close(null, stmt, null);
		}
	}

	/**
	 * @brief 임시테이블의 데이터를 select하여 목적 테이블에 삽입하는 메소드
	 * @param filePath
	 *            테이블에 삽입할 파일이 위치한 경로
	 * @param tableName
	 *            하이브 테이블 명
	 * @throws SQLException
	 */
	public void insertSelect(String fromTable, String toTable)
			throws SQLException {
		log.info(TimerUtil.getTime() + "인서트 시작");
		Connection con = null;
		Statement stmt = null;

		try {
			con = getConnection();
			stmt = con.createStatement();
			String sql = "insert into table " + toTable
					+ " partition(nrpt_dataset_nelo1_id___="
					+ System.currentTimeMillis() + ") select * from "
					+ fromTable;
			log.info(TimerUtil.getTime() + sql);
			stmt.execute(sql);
			log.info(TimerUtil.getTime() + "insertSelect 성공");
		} catch (Exception e) {
			e.printStackTrace();
			log.warn(e.getMessage());
		} finally {
			close(null, stmt, null);
		}
	}

	/**
	 * @brief 파일경로와 테이블명을 인자로 받아 해당 파일을 테이블에 삽입하는 메소드
	 * @param filePath
	 *            테이블에 삽입할 파일이 위치한 경로
	 * @param tableName
	 *            하이브 테이블 명
	 * @throws SQLException
	 */
	public void insertInto(String destTableName, String srcTableName, String fields) throws SQLException {
		log.info(TimerUtil.getTime() + "인서트 시작");
		Connection con = null;
		Statement stmt = null;
		try {
			con = getConnection();
			stmt = con.createStatement();
			StringBuffer sql = new StringBuffer();
			sql.append("insert into table ");
			sql.append(destTableName);
			sql.append(" partition ( nrpt_").append(destTableName).append("_id___ = ").append(System.currentTimeMillis()).append(" ) ");
			sql.append(" select ").append(fields);
			sql.append(" from ").append(srcTableName);
			log.info(TimerUtil.getTime() + sql.toString());
			stmt.execute(sql.toString());
			log.info(TimerUtil.getTime() + "인서트 성공");
		} catch (Exception e) {
			e.printStackTrace();
			log.warn(e.getMessage());
		} finally {
			close(null, stmt, null);
		}
	}

	/**
	 * @brief 테이블을 생성하는 메소드\n 맨 앞 칼럼은 자동적으로 yyyymmdd형식의 date_str string이 입력된다.\n
	 *        맨 마지막 칼럼은 피그분석의 결과로 얻어진 count값(int)가 입력된다.
	 * @param tableName
	 *            하이브 테이블 명
	 * @param fields
	 *            피그 분석 대상이었던 필드명
	 * @throws SQLException
	 */
	public void create(String tableName, String[] fields) throws SQLException {
		log.info(TimerUtil.getTime() + "create 시작");
		Connection con = null;
		Statement stmt = null;
		try {
			con = getConnection();
			stmt = con.createStatement();
			stmt.execute("drop table if exists " + tableName);
			String query = "create table " + tableName + " (";
			for (int i = 0; i < fields.length; i++) {
				if (fields[i].equals("Location")) {
					query += fields[i] + "_str string,";
				} else {
					query += fields[i] + " string,";
				}
			}
			query += "count int) row format delimited fields terminated by '\t' lines terminated by '\n' stored as textfile";
			log.info(query);
			log.info(TimerUtil.getTime() + query);
			stmt.execute(query);
			log.info(TimerUtil.getTime() + "create 성공");
		} catch (Exception e) {
			e.printStackTrace();
			log.warn(e.getMessage());
		} finally {
			close(null, stmt, null);
		}
	}

	/**
	 * @brief 현재 사용되지 않음, 테이블 생성과 데이터 삽입을 동시에 수행하는 메소드
	 * @param tableName
	 *            하이브 테이블 명
	 * @param fields
	 *            피그 분석 대상이었던 필드명
	 * @param filePath
	 *            테이블에 삽입할 파일이 위치한 경로
	 * @throws SQLException
	 */
	public void createAndLoad(String tableName, String[] fields, String filePath)
			throws SQLException {
		log.info(TimerUtil.getTime() + "create and load 시작");
		Connection con = null;
		Statement stmt = null;
		try {
			con = getConnection();
			stmt = con.createStatement();
			stmt.execute("drop table if exists " + tableName);
			String query = "create table " + tableName + " (date_str string,";
			for (int i = 0; i < fields.length; i++) {
				if (fields[i].equals("Location")) {
					query += fields[i] + "_str string,";
				} else {
					query += fields[i] + " string,";
				}
			}
			query += "count int) row format delimited fields terminated by '\t' lines terminated by '\n' stored as textfile";
			log.info(TimerUtil.getTime() + query);
			stmt.execute(query);
			// stmt.execute("create table "+tableName+"(date string, projectVersion string,projectName string,Location string,StabilityCode string,UserId string,DeviceModel string,NetworkType string,Carrier string,logLevel string,CountryCode string,Platform string, count string) row format delimited fields terminated by '\t' lines terminated by '\n' stored as textfile");
			String sql = "load data inpath '" + filePath + "' into table "
					+ tableName;
			stmt.execute(sql);
			log.info(TimerUtil.getTime() + "create and load 성공");
		} catch (Exception e) {
			e.printStackTrace();
			log.warn(e.getMessage());
		} finally {
			close(null, stmt, null);
		}
	}

	/**
	 * @brief 공통으로 쓰이는 자원반환 메소드
	 * @param rset
	 * @param stmt
	 * @param con
	 * @throws SQLException
	 */
	public void close(ResultSet rset, Statement stmt, Connection con)
			throws SQLException {
		if (rset != null)
			try {
				rset.close();
			} catch (Exception e) {
				e.printStackTrace();
				log.warn(e.getMessage());
			}
		if (stmt != null)
			try {
				stmt.close();
			} catch (Exception e) {
				e.printStackTrace();
				log.warn(e.getMessage());
			}
		if (con != null)
			try {
				con.close();
			} catch (Exception e) {
				e.printStackTrace();
				log.warn(e.getMessage());
			}
	}
}