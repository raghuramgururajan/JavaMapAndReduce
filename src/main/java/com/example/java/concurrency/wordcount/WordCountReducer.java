package com.example.java.concurrency.wordcount;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.log4j.Logger;
/**
 * Class to basically reduce all the intermediate output to produce final output
 * @author rgururajan
 *
 */
public class WordCountReducer {

	private static final Integer FIRST_OCCURENCE = 1;
	
	
	private static Logger log = Logger.getLogger(
			WordCountReducer.class.getName());
	/**
	 * Method to basically reduce all the intermediate output to produce final output
	 * @param wordCountFutureList
	 * @return
	 */
	public Map<String,Integer> reduce(List<Future<WordCountStatus>> wordCountFutureList) {
		// TODO Auto-generated method stub
		Map<String,Integer> wordCountMap=new HashMap<String,Integer>();
		try {
		for(Future<WordCountStatus> future:wordCountFutureList)
		{
			
				Map<String,Integer> tempWordCountMap=future.get().getInputWordsMap();
				
				Iterator wordMapIter=tempWordCountMap.keySet().iterator();
				
				while(wordMapIter.hasNext())
				{
					String inputWord=(String) wordMapIter.next();
					if(wordCountMap.isEmpty()|| !wordCountMap.containsKey(inputWord))
					{
						wordCountMap.put(inputWord, FIRST_OCCURENCE);
					}
					else
					{
					    
						int occurence=wordCountMap.get(inputWord);
						occurence++;
						wordCountMap.put(inputWord, occurence);
					}
					
				}
			
			
		}
			
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			log.error("Error occured in WordCountReducer"+e.toString());
		} catch (ExecutionException e) {
			// TODO Auto-generated catch block
			log.error("Error occured in WordCountReducer"+e.toString());

		}
		
		return wordCountMap;
		
	}
	
}
