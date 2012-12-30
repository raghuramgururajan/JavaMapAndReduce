JavaMapAndReduce
================

Java Concurrency API's to do map and reduce. The project basically aims at illustrating the capabilities of Java Concurrency API's(forka and Join)
and how they can be used to perform Map and Reduce operation similar to Hadoop Map/Reduce algorithm.

The project is organized into four different sections namely

a)WordCountProcessor.java- The above class acts as a central point of invocation and the class basically reads the input file,determines the number of active threads available in the operating system .
The input file is uniformly distributed across different threads which internally act as mappers (WordCountMapper)

b)WordCountMapper.java The class basically takes an input list of lines from the input file in the form of a list and tokenizes the list and extracts the input words and then basically adds them to map along with the count of occurrence

c)WordCountStatus.java The class basically contains the processed map from the WordCountMapper.java and each Callable Process(WordCountMapper) returns an object of this class type that contains the processed map with the word count details

c)WordCountReducer.java After all the invocation of different threads(WordCountMapper) are complete then the results are integrated by WordCountReducer.Each Callable Process(WordCountMapper) returns back a custom WordCountStatus that basically contains the list of words processed by each mapper along with the counts.The reducer goes over all the maps and aggregates the count and produces one final map with all word counts


The entire project can be imported as a maven project along with all the resources and there is a sample test included as
part of the project (WordCountMapperReducerTest.java) that can be run to see how the mappers and reducers work.If there is any
feedback please send to me directly at raghuram.gururajan@gmail.com
