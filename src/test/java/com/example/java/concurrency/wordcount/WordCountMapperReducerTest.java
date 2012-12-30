package com.example.java.concurrency.wordcount;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import junit.framework.TestCase;

public class WordCountMapperReducerTest extends TestCase{
	
	/**
	 * Input FileName
	 */
	String inputFileName="sample.txt";
	/**
	 * Word count processor instance
	 */
	WordCountProcessor wcp=null;
	/**
     * Create the test case
     *
     * @param testName name of the test case
     */
	public WordCountMapperReducerTest (String testname)
	{
		super(testname);
	}
	
	/**
	 * Initialize the input file
	 */
	public void setUp()
	{
		wcp=new WordCountProcessor();
		
	}
	
	/**
	 * Test case for testing the output of map and reduce process
	 */
     public void testWordCount()
     {
    	Map<String,Integer> wordCountMap=wcp.processMapAndReduce(inputFileName) ;
    	 assertNotNull(wordCountMap);
    	 assertFalse(wordCountMap.isEmpty());
    	 wcp.displayWordCounts(wordCountMap);
     }
}
