import org.apache.hadoop.conf.Configuration;
import scala.Tuple2;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

public final class JavaWordCount {
    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) throws Exception {

        String myAccessKey = "3c016571be9b44758d4fa73f1da1daf9";
        String mySecretKey = "7129066747854c63bf7054967ad69bd8";
        String bucket = "textfile";
        String filepath = "novel.txt";
        String filepath_result = "result.txt"; 
        SparkSession spark = SparkSession
                .builder()
                .appName("JavaWordCount")
                .getOrCreate();

        Configuration hadoopConf = spark.sparkContext().hadoopConfiguration();
        hadoopConf.set("fs.s3a.access.key", myAccessKey);
        hadoopConf.set("fs.s3a.secret.key", mySecretKey);
        hadoopConf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        hadoopConf.set("fs.s3a.endpoint","s3.gra.cloud.ovh.net"); 
        hadoopConf.set("fs.s3a.path.style.access", "true");

        JavaRDD<String> lines = spark.read().textFile("s3a://" + bucket + "/" + filepath).javaRDD();
        JavaRDD<String> words = lines.flatMap(s -> Arrays.asList(SPACE.split(s)).iterator());
        JavaPairRDD<String, Integer> ones = words.mapToPair(s -> new Tuple2<>(s, 1));
        JavaPairRDD<String, Integer> counts = ones.reduceByKey((i1, i2) -> i1 + i2);
        
        counts.saveAsTextFile("s3a://" + bucket + "/" + filepath_result);
        List<Tuple2<String, Integer>> output = counts.collect();
        for (Tuple2<?,?> tuple : output) {
            System.out.println(tuple._1() + ": " + tuple._2());
        }
        spark.stop();
    }
}