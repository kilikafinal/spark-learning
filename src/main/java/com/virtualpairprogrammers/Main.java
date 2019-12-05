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

    System.setProperty("hadoop.home.dir", "c:/hadoop");
    Logger.getLogger("org.apache").setLevel(Level.WARN);

    SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
    JavaSparkContext sc = new JavaSparkContext(conf);

    List<Tuple2<Integer,Integer>> visitsRaw = new ArrayList<>();
    visitsRaw.add(new Tuple2<>(4,18));
    visitsRaw.add(new Tuple2<>(6,4));
    visitsRaw.add(new Tuple2<>(10,9));

      List<Tuple2<Integer,String>> usersRaw = new ArrayList<>();
      usersRaw.add(new Tuple2<>(1, "John"));
      usersRaw.add(new Tuple2<>(2, "Bob"));
      usersRaw.add(new Tuple2<>(3, "Alan"));
      usersRaw.add(new Tuple2<>(4, "Doris"));
      usersRaw.add(new Tuple2<>(5, "Marybelle"));
      usersRaw.add(new Tuple2<>(6, "Rachel"));
      usersRaw.add(new Tuple2<>(7, "asdf"));

      JavaPairRDD<Integer, Integer> visits = sc.parallelizePairs(visitsRaw);
      JavaPairRDD<Integer, String> users = sc.parallelizePairs(usersRaw);

      JavaPairRDD<Integer, Tuple2<Integer, String>> joinedRdd = visits.join(users);

    joinedRdd.collect().forEach(System.out::println);

      sc.close();
  }
}
