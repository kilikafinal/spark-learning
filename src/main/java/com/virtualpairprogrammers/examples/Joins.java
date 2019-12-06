package com.virtualpairprogrammers.examples;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class Joins {

  private String path = "src/main/resources/subtitles/input.txt";
  private SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");

  private List<Tuple2<Integer, Integer>> visitsRaw = new ArrayList<>();
  private List<Tuple2<Integer, String>> usersRaw = new ArrayList<>();

  public Joins() {

    visitsRaw.add(new Tuple2<>(4, 18));
    visitsRaw.add(new Tuple2<>(6, 4));
    visitsRaw.add(new Tuple2<>(10, 9));

    usersRaw.add(new Tuple2<>(1, "John"));
    usersRaw.add(new Tuple2<>(2, "Bob"));
    usersRaw.add(new Tuple2<>(3, "Alan"));
    usersRaw.add(new Tuple2<>(4, "Doris"));
    usersRaw.add(new Tuple2<>(5, "Marybelle"));
    usersRaw.add(new Tuple2<>(6, "Rachel"));
    usersRaw.add(new Tuple2<>(7, "asdf"));
  }

  public void leftOuterJoin() {
    JavaSparkContext sc = new JavaSparkContext(conf);
    Logger.getLogger("org.apache").setLevel(Level.WARN);

    JavaPairRDD<Integer, Integer> visits = sc.parallelizePairs(visitsRaw);
    JavaPairRDD<Integer, String> users = sc.parallelizePairs(usersRaw);
    JavaPairRDD<Integer, Tuple2<Integer, Optional<String>>> joinedRdd = visits.leftOuterJoin(users);

    joinedRdd.collect().forEach(it -> System.out.println(it._2._2.orElse("null value")));

    sc.close();
  }

  public void rightOuterJoin() {
    JavaSparkContext sc = new JavaSparkContext(conf);
    Logger.getLogger("org.apache").setLevel(Level.WARN);
    JavaPairRDD<Integer, Integer> visits = sc.parallelizePairs(visitsRaw);
    JavaPairRDD<Integer, String> users = sc.parallelizePairs(usersRaw);

    JavaPairRDD<Integer, Tuple2<Optional<Integer>, String>> joinedRdd =
        visits.rightOuterJoin(users);

    joinedRdd.collect().forEach(it -> System.out.println(it._2._2));

    sc.close();
  }
}
