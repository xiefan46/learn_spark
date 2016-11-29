import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.File;
import java.util.Arrays;
import java.util.Iterator;

/**
 * Created by xiefan on 16-11-29.
 */
public class WordCount {

    public static void main(String[] args) throws Exception{
        SparkConf conf = new SparkConf().setAppName("WordCount").setMaster("spark://sandbox.hortonworks.com:7077");
        JavaSparkContext sc = new JavaSparkContext(conf);
        //JavaRDD<String> input = sc.parallelize(Arrays.asList("hello world. hello me hello"));
        JavaRDD<String> input = sc.textFile("src/main/resources/test.txt");
        JavaRDD<String> words = input.flatMap(new FlatMapFunction<String, String>() {
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" ")).iterator();
            }
        });
        JavaPairRDD<String,Integer> counts = words.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s,1);
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer x, Integer y) throws Exception {
                return x + y;
            }
        });
        //counts.saveAsTextFile("./wordCount.out");
        System.out.println(counts.collect());
    }
}
