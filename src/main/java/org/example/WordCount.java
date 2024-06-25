package org.example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class WordCount {
    public static void main(String[] args) {
        // Check inputs
        if (args.length < 2) {
            System.err.println("Missing input/output path");
            System.exit(1);
        }

        // Parse inputs
        String inputPath = args[0];
        String outputPath = args[1];

        // Init sparkConfig and SparkContext
        SparkConf sparkConf = new SparkConf().setAppName("WordCount");
        sparkConf.set("spark.driver.extraJavaOptions", "-XX:+UseG1GC");
        sparkConf.set("spark.executor.extraJavaOptions", "-XX:+UseG1GC");

        try (JavaSparkContext sparkContext = new JavaSparkContext(sparkConf)) {
            JavaRDD<String> textFile = sparkContext.textFile(inputPath).cache();

            // Load data from file, distribute and process data
            JavaPairRDD<Integer, Integer> javaPairRDDWordLengthCount = textFile
                    .flatMap(s -> Arrays.asList(s.split("\\s+")).iterator())
                    .filter(word -> {
                        int length = word.length();
                        return length >= 5 && length <= 9;
                    })
                    .mapToPair(word -> new Tuple2<>(word.length(), 1))
                    .reduceByKey(Integer::sum);

            // Collect data
            List<Tuple2<Integer, Integer>> wordLengthCount = javaPairRDDWordLengthCount.collect();

            // Write Output to file
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputPath))) {
                writer.write("The words have length from 5 to 9 chars:\n\n");
                for (Tuple2<Integer, Integer> row : wordLengthCount) {
                    writer.write(  "- " + row._1() + ": " + row._2() + "\n");
                }
            } catch (IOException e) {
                System.err.println("Error writing to output file: " + e.getMessage());
            }
        } catch (Exception e) {
            System.err.println("Error during Spark execution: " + e.getMessage());
        }
    }
}
