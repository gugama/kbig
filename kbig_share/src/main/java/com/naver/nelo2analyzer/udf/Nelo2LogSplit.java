package com.naver.nelo2analyzer.udf;

import java.io.IOException;
import java.net.URLDecoder;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.PatternSyntaxException;

import org.apache.pig.EvalFunc;
import org.apache.pig.PigWarning;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

/**
 * @class Nelo2LogSplit
 * @brief {key:value key:value ...}형태의 로그를 파싱하는 Pig UDF입니다.
 * @file Nelo2LogSplit.java
 * @brief {key:value key:value ...}형태의 로그를 파싱하는 Pig UDF입니다.
 */
public class Nelo2LogSplit extends EvalFunc<Tuple> {
	public Nelo2LogSplit() {
	}

	private final static TupleFactory tupleFactory = TupleFactory.getInstance();

	@Override
	public Tuple exec(Tuple input) throws IOException {
		if (input == null || input.size() < 1)
			return null;
		try {
			ArrayList<Object> protoTuple = new ArrayList<Object>();
			String source = (String) input.get(0);
			String delim1 = (input.size() > 1) ? (String) input.get(1) : "\\s";
			String delim2 = (input.size() > 2) ? (String) input.get(2) : "\\s";
			String[] splits1 = source.split(delim1);// key:value
			Map<String, String> attributes = new HashMap<String, String>();
			for (int i = 0; i < splits1.length; i++) {
				try {
					attributes.put(splits1[i].split(delim2)[0],
							splits1[i].split(delim2)[1]);
				} catch (Exception e) {
					System.err.println("에러발생지점 " + splits1[i]);
					e.printStackTrace();
					// attributes.put("error", "error");
				}
			}
			String target;
			System.err.println("대상필드 보유했는지 여부 판단 돌기전");
			for (int i = 3; i < input.size(); i++) {
				target = (String) input.get(i);
				if (target.equals("logTime")) {
					if (attributes.get(target) != null) {
						SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");
						Date date = new Date(Long.parseLong(attributes
								.get(target))*1000);
						protoTuple.add(df.format(date));
					} else {
						protoTuple.add("NULL");
					}
				} else if (attributes.containsKey(target)) {
					protoTuple.add(URLDecoder.decode(attributes.get(target),
							"UTF-8"));
				} else {
					protoTuple.add("NULL");
				}
			}
			return tupleFactory.newTupleNoCopy(protoTuple);
		} catch (ClassCastException e) {
			warn("class cast exception at " + e.getStackTrace()[0],
					PigWarning.UDF_WARNING_1);
		} catch (PatternSyntaxException e) {
			warn(e.getMessage(), PigWarning.UDF_WARNING_1);
		}
		// this only happens if the try block did not complete normally
		return null;
	}
}
