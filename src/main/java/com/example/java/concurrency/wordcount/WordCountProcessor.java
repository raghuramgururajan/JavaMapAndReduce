package com.example.java.concurrency.wordcount;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
/**
 * This class is basically used to read the sample file and then configure the thread pools,determine number of threads available and 
 * then delegate the work to WordCountMapper and WordCountReducer and display the final output of wordcount
 * @author raghuramgururajan
 *
 */
public class WordCountProcessor {

	

	Properties config = null;

	private static Logger log = Logger.getLogger(WordCountProcessor.class
			.getName());

	/**
	 * This method basically reads the input file and calls mapper to process
	 * the data.There are multiple threads that basically process the input and
	 * one reducer that basically finally merges the input
	 * 
	 * @param inputFileName
	 * @return
	 */
	public Map<String, Integer> processMapAndReduce(String inputFileName) {
		BufferedReader br = null;
		FileReader fr = null;
		Map<String, Integer> wordCountMap = null;
		try {
			File inputFile = new File(
					(this.getClass().getClassLoader()
							.getResource(inputFileName)).toURI());
			fr = new FileReader(inputFile);
			// URLConnection yc = oracle.openConnection();
			br = new BufferedReader(fr);

			long contentLength = inputFile.length();
			// Number of threads are determined from run time
			int numberOfThreads = Runtime.getRuntime().availableProcessors();
			// Thread pool executor initialized
			ThreadPoolExecutor threadpoolExecutor = new ThreadPoolExecutor(
					numberOfThreads, numberOfThreads, 10, TimeUnit.SECONDS,
					new LinkedBlockingQueue<Runnable>());
			// Thread pool executor invoked
			wordCountMap = invokeThreadExecutor(threadpoolExecutor,
					contentLength, numberOfThreads, br);

		} catch (IOException e) {
			log.error("IOException occured in WordCountProcessor:: processMapAndReduce"
					+ e.toString());
		} catch (URISyntaxException e) {
			// TODO Auto-generated catch block
			log.error("URISyntaxException occured in WordCountProcessor:: processMapAndReduce"
					+ e.toString());
		}

		finally {
			IOUtils.closeQuietly(br);
			IOUtils.closeQuietly(fr);

		}
		return wordCountMap;

	}

	/**
	 * Method to actually create and invoke multiple threads to process the
	 * input word set
	 * 
	 * @param threadpoolExecutor
	 * @param contentLength
	 * @param numberOfThreads
	 * @param in
	 * @return
	 * @throws IOException
	 */
	private Map<String, Integer> invokeThreadExecutor(
			ThreadPoolExecutor threadpoolExecutor, long contentLength,
			int numberOfThreads, BufferedReader br) throws IOException {
		// TODO Auto-generated method stub

		List<Callable<WordCountStatus>> wordCountTasks = new ArrayList<Callable<WordCountStatus>>();
		int beginOffset = 0;
		int endOffset = 0;
		Map<String, Integer> wordCountMap = null;
		List<Future<WordCountStatus>> wordCountFutureList = null;
		try {

			List<String> inputWordsList = getInputList(br);

			int contentLengthPerThread = getContentLengthPerThread(
					numberOfThreads, inputWordsList);

			endOffset = contentLengthPerThread;
			System.out
					.println("::::::::::::::::::::::::::::::::::Calling Mapper to map the words::::::::::::::::::::::::::::::::");
			for (int i = 0; i < numberOfThreads; i++) {
				System.out.println((((float) (i + 1) / numberOfThreads) * 100)
						+ "%  mapping complete");
				if (!inputWordsList.isEmpty()) {

					List<String> subListInputWords = inputWordsList.subList(
							beginOffset, endOffset);
					WordCountMapper wcm = new WordCountMapper(subListInputWords);
					wordCountTasks.add(wcm);
					beginOffset = endOffset;
					endOffset = endOffset + contentLengthPerThread;
					if (beginOffset >= contentLength
							|| beginOffset == endOffset
							|| endOffset > inputWordsList.size()) {
						break;
					}
					if (endOffset > contentLength) {
						endOffset = (int) contentLength;
					}

				}
			}

			wordCountFutureList = threadpoolExecutor.invokeAll(wordCountTasks);
			WordCountReducer wcr = new WordCountReducer();
			System.out
					.println("::::::::::::::::::::::::::::::::Calling reducer to reduce the words::::::::::::::::::::::::::::::::");
			wordCountMap = wcr.reduce(wordCountFutureList);
			threadpoolExecutor.shutdown();

		} catch (IndexOutOfBoundsException e) {
			// TODO Auto-generated catch block
			log.error("Exception occured while invoking the mapper threads"
					+ e.toString());

			// System.out.println("ContentLength:::"+contentLengthPerThread);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			log.error("Exception occured while invoking the mapper threads"
					+ e.toString());
		}

		return wordCountMap;
	}

	private int getContentLengthPerThread(int numberOfThreads,
			List<String> inputWordsList) {
		// TODO Auto-generated method stub
		int contentLengthPerThread = 0;
		if (numberOfThreads == 0) {
			contentLengthPerThread = 1;
			numberOfThreads = 1;
		} else {
			contentLengthPerThread = inputWordsList.size() / numberOfThreads;
		}
		if (contentLengthPerThread == 0) {
			contentLengthPerThread = inputWordsList.size();
		}
		return contentLengthPerThread;
	}

	private List<String> getInputList(BufferedReader br) throws IOException {
		// TODO Auto-generated method stub
		ArrayList<String> inputWordsList = new ArrayList<String>();
		String tempInputWord = null;
		while ((tempInputWord = br.readLine()) != null) {
			inputWordsList.add(tempInputWord);
		}
		return inputWordsList;
	}

	/**
	 * Method to display word counts
	 * 
	 * @param wordCountMap
	 */
	public void displayWordCounts(Map<String, Integer> wordCountMap) {
		Iterator wordCountsIter = wordCountMap.keySet().iterator();

		System.out.println("WordName" + " :: :: " + "WordCount");

		while (wordCountsIter.hasNext()) {
			String inputWord = (String) wordCountsIter.next();

			System.out.println(inputWord + " :: :: "
					+ wordCountMap.get(inputWord));
		}
	}

	public static void main(String[] args) {
		WordCountProcessor wcp = new WordCountProcessor();
		wcp.processMapAndReduce("sample.txt");

	}

}
