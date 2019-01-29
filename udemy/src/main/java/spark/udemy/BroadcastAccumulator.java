package spark.udemy;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.LongAccumulator;

import scala.Tuple2;

public class BroadcastAccumulator {

	static String appName = "Udemy-KeyValue";
	static String master = "local";

	public static void main(String[] args) {
		Logger.getLogger("org").setLevel(Level.ERROR);
		Logger.getLogger("akka").setLevel(Level.ERROR);
		
		Utilities util = new Utilities();
		
		SparkConf conf = new SparkConf().setAppName(appName).setMaster(master);
		try (JavaSparkContext context = new JavaSparkContext(conf)) {
			JavaRDD<String> irisRDD = context.textFile("data/iris.csv");
			
			String header = irisRDD.first();
			JavaRDD<String> irisRDD2 = irisRDD.filter(x -> !x.contains(header));
			
			String lengthTotal = irisRDD2.map(x -> x.split(",")[0])
					.reduce((x, y) -> String.valueOf(Float.valueOf(x) + Float.valueOf(y)));
			float count = irisRDD2.count();

			Broadcast<Float> avg = context.broadcast(Float.valueOf(lengthTotal) / count);
			LongAccumulator aboveAvgCount = context.sc().longAccumulator();
			
			JavaPairRDD<String, Float> values = irisRDD2.mapToPair(new PairFunction<String, String, Float>(){
				public Tuple2<String, Float> call(String str){
					Tuple2<String, Float> tuple;
					String[] valueList = str.split(",");
					if(Float.valueOf(valueList[0]) > avg.value()) {
						aboveAvgCount.add(1);
					}
					return new Tuple2<String, Float>(valueList[4], Float.valueOf(valueList[0])); 
					
				}
			});
			
			System.out.println("Average Sepal Length is " + aboveAvgCount.value());
			
			//Filter RDDs those are greater than avg
			/*JavaPairRDD<String, Float> aboveAvg = values.filter(new Function<Tuple2<String, Float>, Boolean>() {
				public Boolean call(Tuple2<String, Float> item) {
					return item._2 > avg.value();
				}
			});*/	
		}
	}

}
