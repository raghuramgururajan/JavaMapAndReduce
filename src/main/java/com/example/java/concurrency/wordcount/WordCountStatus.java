package com.example.java.concurrency.wordcount;

import java.util.HashMap;
import java.util.Map;

/**
 * Class to store the status of all the intermediate tasks and act
 * as an input for reducer for final word count computation
 * @author rgururajan
 *
 */
public class WordCountStatus {
	
	private Map<String,Integer> inputWordsMap=null;

	public Map<String, Integer> getInputWordsMap() {
		return inputWordsMap;
	}

	public void setInputWordsMap(Map<String, Integer> inputWordsMap) {
		this.inputWordsMap = inputWordsMap;
	}

}
