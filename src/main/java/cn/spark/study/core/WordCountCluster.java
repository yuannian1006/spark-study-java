package cn.spark.study.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;

/**
 * 本地测试的WordCount
 * Created by NianYuan on 2017/9/13.
 */
public class WordCountCluster {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("WordCountCluster");

        //创建javaContext对象
        JavaSparkContext sc = new JavaSparkContext(conf);
        //创建RDD
        JavaRDD<String> lines = sc.textFile("hdfs://com.yuannian04/input/emp.txt");
        //transformation操作
        
        //将每行拆分一个个单词
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            private static final long serialVersionUID = 1L;

            public Iterable<String> call(String line) throws Exception {
                return Arrays.asList(line.split("\t"));
            }
        });
        //words映射为(word,1)
        JavaPairRDD<String,Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
            private static final long serialVersionUID = 1L;
            public Tuple2<String ,Integer> call (String word) throws Exception{
                return new Tuple2<String, Integer>(word,1);
            }
        });
        //统计单词出现的次数
        JavaPairRDD<String, Integer> wordCounts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            private static final long serialVersionUID = 1L;
            public Integer call(Integer v1, Integer v2) throws Exception {

                return v1 + v2;
            }
        });
        //action操作
        wordCounts.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            public void call(Tuple2<String, Integer> wordCount) throws Exception {
                System.out.println(wordCount._1() + " " + wordCount._2());
            }
        });

        sc.close();

    }
}
