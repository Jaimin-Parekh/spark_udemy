package spark.udemy;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

public class Transformation {

	static String appName = "Udemy-Transformation";
	static String master = "local";

	public static void main(String[] args) {
		// Setting loggers to ERROR level
		Logger.getLogger("org").setLevel(Level.ERROR);
		Logger.getLogger("akka").setLevel(Level.ERROR);

		Utilities util = new Utilities();

		SparkConf conf = new SparkConf().setAppName(appName).setMaster(master);
		try (JavaSparkContext context = new JavaSparkContext(conf)) {

			JavaRDD<String> irisRDD = context.textFile("data/iris.csv");
			irisRDD.cache();

			util.printRDD(floatConversion(irisRDD), 10);
			util.printRDD(filterRDD(irisRDD, "versicolor"), 10);

			// Uncomment the following line to monitor Spark Job in webui
			// util.observe(true);
		}
	}

	// Either make methods static or implement @Serializable
	static JavaRDD<String> floatConversion(JavaRDD<String> rdd) {
		String header = rdd.first();

		JavaRDD<String> resultRDD = rdd.map(new Function<String, String>() {
			// private static final long serialVersionUID = 3305991747267854123L;

			public String call(String text) {
				String floatValues = "";
				if (text.equals(header))
					return text;
				else {
					String[] values = text.split(",");
					for (int i = 0; i < values.length - 1; i++) {
						if (i == 0)
							floatValues += String.valueOf(Float.parseFloat(values[i]));
						else
							floatValues += "," + String.valueOf(Float.parseFloat(values[i]));
					}
					return floatValues;
				}
			}
		});
		return resultRDD;
	}

	static JavaRDD<String> filterRDD(JavaRDD<String> inputRDD, String filterString) {
		return inputRDD.filter(x -> x.contains(filterString));
	}
}