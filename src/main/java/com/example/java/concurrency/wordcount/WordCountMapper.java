/**
 * 
 */
package com.example.java.concurrency.wordcount;

import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * This class is used to map the input words by creating a map of all input words along with their 
 * count of occurence.
 * @author rgururajan
 *
 */
public class WordCountMapper implements Callable<WordCountStatus> {
	private static final CharSequence PATTERN_COMMA = ",";
	private static final String PATTERN_SPACE = " ";
	private static final CharSequence PATTERN_EMPTY_STRING = "";
	private static final CharSequence PATTERN_DOT = ".";
	private static final Integer FIRST_OCCURENCE = 1;
	private static final int LENGTH_ONE = 1;
	private List<String> inputWordsList=null;
	public WordCountMapper(List<String> inputWordsList)
	{
		this.inputWordsList=inputWordsList;
	}
	public WordCountStatus call() throws Exception {
		// TODO Auto-generated method stub
		WordCountStatus ws=new WordCountStatus();
		Map<String,Integer> inputWordsMap=new HashMap<String,Integer>();
		//Iterate over the inputWordsList and create a intermediate map with counts
		for(String inputWords:inputWordsList)
		{	
		StringTokenizer inputWordsTokens=new StringTokenizer(inputWords,PATTERN_SPACE);
		
		while(inputWordsTokens.hasMoreTokens())
		{
			String inputWordMapKey=inputWordsTokens.nextToken();
			
			//Filter and remove commas and empty spaces
			if(inputWordMapKey!=null && inputWordMapKey.length() > LENGTH_ONE)
			{
				if(inputWordMapKey.contains(PATTERN_COMMA))
				{
					inputWordMapKey=inputWordMapKey.replace(PATTERN_COMMA,PATTERN_EMPTY_STRING);
				}
				if(inputWordMapKey.contains(PATTERN_DOT))
				{
					inputWordMapKey=inputWordMapKey.replace(PATTERN_DOT,PATTERN_EMPTY_STRING);
				}
				//If the map is not empty and if the map already contains the word increment
				//the counter
				inputWordMapKey=inputWordMapKey.toLowerCase();
				if(!inputWordsMap.isEmpty() && inputWordsMap.containsKey(inputWordMapKey))
				{
					int wordCount=inputWordsMap.get(inputWordMapKey);
					wordCount++;
					inputWordsMap.put(inputWordMapKey, wordCount);
				}
				else
				{
					inputWordsMap.put(inputWordMapKey, FIRST_OCCURENCE);
				}
			}
			
		}
		}
		//Validate the map to see if its empty or not
		if(!inputWordsMap.isEmpty())
		{
			ws.setInputWordsMap(inputWordsMap);
		}
		
		return ws;
	}
	

}
