package com.virtualpairprogrammers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
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
    List<String> inputData = new ArrayList<>();
    inputData.add("WARN: Tuesday 4 September 0405");
    inputData.add("ERROR: Tuesday 4 September 0408");
    inputData.add("FATAL: Wednesday 5 September 1632");
    inputData.add("ERROR: Friday 7 September 1854");
    inputData.add("WARN: Saturday 8 September 1942");

    Logger.getLogger("org.apache").setLevel(Level.WARN);

    SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
    JavaSparkContext sc = new JavaSparkContext(conf);

    String path = "src/main/resources/test.txt";
    sc.textFile(path)
        .flatMap(value -> Arrays.asList(value.split(" ")).iterator())
        .collect()
        .forEach(System.out::println);

    sc.close();
  }
}
