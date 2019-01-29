package spark.udemy;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Action {

	static String appName = "Udemy-Action";
	static String master = "local";

	public static void main(String[] args) {
		Logger.getLogger("org").setLevel(Level.ERROR);
		Logger.getLogger("akka").setLevel(Level.ERROR);

		Utilities util = new Utilities();

		SparkConf conf = new SparkConf().setAppName(appName).setMaster(master);
		try (JavaSparkContext context = new JavaSparkContext(conf)) {

			JavaRDD<String> file = context.textFile("data/iris.csv");
			JavaRDD<String> irisRDD = util.filterHeader(file);

			String lengthTotal = irisRDD.map(x -> x.split(",")[0])
					.reduce((x, y) -> String.valueOf(Float.valueOf(x) + Float.valueOf(y)));

			float count = irisRDD.count();
			
			
			System.out.println("Average Sepal Length is " + Float.valueOf(lengthTotal) / count);

			// Uncomment the following line to monitor Spark Job in webui
			// util.observe(true);
		}		
	}
}