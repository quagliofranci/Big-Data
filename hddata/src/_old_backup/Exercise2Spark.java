import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Calendar;

public class Exercise2Spark {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("WebConversionRate");
        JavaSparkContext sc = new JavaSparkContext(conf);
        String inputPath = args[0];
        String outputPath = args[1];
        JavaRDD<String> lines = sc.textFile(inputPath);
        String header = lines.first();
        JavaRDD<String> data = lines.filter(line -> !line.equals(header));

        JavaPairRDD<Integer, Tuple2<Double, Double>> yearlyData = data
            .mapToPair(line -> {
                String[] cols = line.split(",");
                if (cols.length <= 28 || !cols[28].trim().equals("1")) return null;
                try {
                    SimpleDateFormat sdf = new SimpleDateFormat("dd-MM-yyyy");
                    Date date = sdf.parse(cols[7]);
                    Calendar calendar = Calendar.getInstance();
                    calendar.setTime(date);
                    int year = calendar.get(Calendar.YEAR);
                    double wP = Double.parseDouble(cols[16]);
                    double wV = Double.parseDouble(cols[19]);
                    return new Tuple2<>(year, new Tuple2<>(wP, wV));
                } catch (Exception e) { return null; }
            }).filter(t -> t != null);

        JavaPairRDD<Integer, Double> result = yearlyData
            .reduceByKey((v1, v2) -> new Tuple2<>(v1._1()+v2._1(), v1._2()+v2._2()))
            .mapValues(v -> (v._2() == 0) ? 0.0 : (v._1() / v._2()))
            .sortByKey();

        result.saveAsTextFile(outputPath);
        sc.close();
    }
}
