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

import com.naver.nelo2analyzer.util.TimerUtil;

/**
 * @class Nelo2LogSplit
 * @brief {key:value key:value ...}형태의 로그를 파싱하는 Pig UDF입니다.
 * @file Nelo2LogSplit.java
 * @brief {key:value key:value ...}형태의 로그를 파싱하는 Pig UDF입니다.
 */
public class test extends EvalFunc<Tuple> {
	public test() {
	}

	private final static TupleFactory tupleFactory = TupleFactory.getInstance();

	@Override
	public Tuple exec(Tuple input) throws IOException {
		if (input == null || input.size() < 1)
			return null;
		try {
			ArrayList<Object> protoTuple = new ArrayList<Object>();
			String source = (String) input.get(0);
			protoTuple.add("text");
			protoTuple.add(TimerUtil.getTime());
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
