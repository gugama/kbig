package com.naver.nelo2analyzer.udf;import java.io.FileNotFoundException;
import java.io.IOException;

import kr.ac.kaist.swrc.jhannanum.hannanum.Workflow;
import kr.ac.kaist.swrc.jhannanum.plugin.MajorPlugin.MorphAnalyzer.ChartMorphAnalyzer.ChartMorphAnalyzer;
import kr.ac.kaist.swrc.jhannanum.plugin.MajorPlugin.PosTagger.HmmPosTagger.HMMTagger;
import kr.ac.kaist.swrc.jhannanum.plugin.SupplementPlugin.MorphemeProcessor.UnknownMorphProcessor.UnknownProcessor;
import kr.ac.kaist.swrc.jhannanum.plugin.SupplementPlugin.PlainTextProcessor.InformalSentenceFilter.InformalSentenceFilter;
import kr.ac.kaist.swrc.jhannanum.plugin.SupplementPlugin.PlainTextProcessor.SentenceSegmentor.SentenceSegmentor;
import kr.ac.kaist.swrc.jhannanum.plugin.SupplementPlugin.PosProcessor.NounExtractor.NounExtractor;

/**
 * @author k 1. 문자열 입력 2. 해당 문자열을 형태소 분석기로 분석 3. 분석한 결과를 출력
 */
public class KeywordExtractorTest {

	public static void main(String[] args) {
		Workflow workflow = new Workflow();

		try {
			//work flow 세팅
			//1. 의미없는 문장 삭제하는 플러그인 세팅
			workflow.appendPlainTextProcessor(new SentenceSegmentor(), null);
			workflow.appendPlainTextProcessor(new InformalSentenceFilter(), null);

			/*
			 * Phase2. Morphological Analyzer Plug-in and Supplement Plug-in for
			 * post processing
			 */
			workflow.setMorphAnalyzer(new ChartMorphAnalyzer(), "hnn/conf/plugin/MajorPlugin/MorphAnalyzer/ChartMorphAnalyzer.json");
			workflow.appendMorphemeProcessor(new UnknownProcessor(), null);

			/*
			 * For simpler morphological analysis result with 22 tags, decomment
			 * the following line. Notice: If you use SimpleMAResult22 plug-in,
			 * POSTagger will not work correctly. So don't add phase3 plug-ins
			 * after SimpleMAResult22.
			 */
			// workflow.appendMorphemeProcessor(new SimpleMAResult22(), null);

			/*
			 * For simpler morphological analysis result with 9 tags, decomment
			 * the following line. Notice: If you use SimpleMAResult09 plug-in,
			 * POSTagger will not work correctly. So don't add phase3 plug-ins
			 * after SimpleMAResult09.
			 */
			// workflow.appendMorphemeProcessor(new SimpleMAResult09(), null);

			/*
			 * Phase3. Part Of Speech Tagger Plug-in and Supplement Plug-in for
			 * post processing
			 */
			workflow.setPosTagger(new HMMTagger(),
					"hnn/conf/plugin/MajorPlugin/PosTagger/HmmPosTagger.json");

			/* For extracting nouns only, decomment the following line. */
			workflow.appendPosProcessor(new NounExtractor(), null);

			/*
			 * For simpler POS tagging result with 22 tags, decomment the
			 * following line.
			 */
			// workflow.appendPosProcessor(new SimplePOSResult22(), null);

			/*
			 * For simpler POS tagging result with 9 tags, decomment the
			 * following line.
			 */
			// workflow.appendPosProcessor(new SimplePOSResult09(), null);

			/* Activate the work flow in the thread mode */
			workflow.activateWorkflow(true);

			/* Analysis using the work flow */
			//String document = "&lt;손바닥 삼국지2&gt;를 필터 플레이 해보자! 이제 노무현 막 여정에 올랐어. 나와 함께하자! 내 추천인 코드“Yaho12”를 입력하면 매우 좋은 선물을 받을 수 있어! http://t.co/yvTmOut1SP";
			String document = "난간 오늘 인피니트 근데 지금 막 진짜";
			
			//workflow.analyze(document);
			//System.out.println(workflow.getResultOfDocument());

/*			 Once a work flow is activated, it can be used repeatedly. 
			document = "日時: 2010년 7월 30일 오후 1시\n"
					+ "場所: Coex Conference Room\n";*/

			workflow.analyze(document);
			String result = workflow.getResultOfDocument();
			String[] results=result.split("\\s");
			String finalResult="";
			for(String s:results){
				if(s.contains("/"))
					finalResult+=s.split("/")[0]+" ";
			}
			System.out.println(finalResult);
			//System.out.println(workflow.getResultOfSentence());

			/* Close the work flow */
			workflow.close();

		} catch (FileNotFoundException e) {
			e.printStackTrace();
			System.exit(0);
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(0);
		} catch (Exception e) {
			e.printStackTrace();
		}

		/* Shutdown the workflow */
		workflow.close();

	}
}
