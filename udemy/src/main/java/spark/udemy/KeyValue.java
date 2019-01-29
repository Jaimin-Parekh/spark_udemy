package spark.udemy;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class KeyValue {

	static String appName = "Udemy-KeyValue";
	static String master = "local";

	public static void main(String[] args) {
		Logger.getLogger("org").setLevel(Level.ERROR);
		Logger.getLogger("akka").setLevel(Level.ERROR);
		
		SparkConf conf = new SparkConf().setAppName(appName).setMaster(master);
		try (JavaSparkContext context = new JavaSparkContext(conf)) {
			JavaRDD<String> irisRDD = context.textFile("data/iris.csv");
			
			String header = irisRDD.first();
			JavaRDD<String> irisRDD2 = irisRDD.filter(x -> !x.contains(header));
			
			
			JavaPairRDD<String, Float> speciesLength = irisRDD2.mapToPair(new PairFunction<String, String, Float>() {
				public Tuple2<String, Float> call(String str) {
					String[] values = str.split(",");
					return new Tuple2<String, Float>(values[4], Float.valueOf(values[0]));
				}
			});
			
			JavaPairRDD<Float, String> swappedRDD = speciesLength.mapToPair(new PairFunction<Tuple2<String, Float>, Float, String>(){
				public Tuple2<Float, String> call(Tuple2<String, Float> item){
					return item.swap();
				}
			});

			JavaPairRDD<Float, String> sortedRDD = swappedRDD.sortByKey();
			System.out.println("Minimum Sepal Length "+sortedRDD.first());
			
			//Swap back to Species, Length
			/*JavaPairRDD<String, Float> sortedRDD2 = sortedRDD.mapToPair(new PairFunction<Tuple2<Float, String>, String, Float>(){
				public Tuple2<String, Float> call(Tuple2<Float, String> item){
					return item.swap();
				}
			});*/
			
			// Uncomment the following line to monitor Spark Job in webui
			// util.observe(true);
		}
	}
}