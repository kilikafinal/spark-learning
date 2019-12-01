package com.virtualpairprogrammers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.codehaus.janino.Java;
import scala.Tuple2;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Main {

    public static void main(String[] args) {

        String path = "src/main/resources/subtitles/input.txt";
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> initialRdd = sc.textFile(path);

        JavaRDD<String> noSpecialCharsRdd = initialRdd.map(sentence -> sentence.replaceAll("[^a-zA-Z\\s]", "").toLowerCase());

        JavaRDD<String> noLinesRdd = noSpecialCharsRdd.filter(sentence -> sentence.trim().length() > 0 );

        JavaRDD<String> dividedWords = noLinesRdd.flatMap(line -> Arrays.asList(line.split(" ")).iterator());

        dividedWords = dividedWords.filter(word -> word.trim().length() > 0);

        JavaRDD<String> interestingWords = dividedWords.filter(words -> Util.isNotBoring(words));

        JavaPairRDD<String, Long> pairRdd = interestingWords.mapToPair(word -> new Tuple2<>(word, 1L));

        JavaPairRDD<String, Long> totals = pairRdd.reduceByKey((value1, value2) -> value1 + value2);

        JavaPairRDD<Long, String> switched = totals.mapToPair(tuple -> new Tuple2<Long, String>(tuple._2, tuple._1));


        JavaPairRDD<Long, String> ordered = switched.sortByKey(false);
        List<Tuple2<Long, String>> results = ordered.take(10);
        results.forEach(System.out::println);

        sc.close();
    }
}
