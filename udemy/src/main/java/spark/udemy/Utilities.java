package spark.udemy;

import org.apache.spark.api.java.JavaRDD;

public class Utilities {
	
	void printRDD(JavaRDD<String> printRDD, int value) {
		for (String str : printRDD.take(value))
			System.out.println(str);
	}

	JavaRDD<String> filterHeader(JavaRDD<String> rdd) {
		String header = rdd.first();
		return rdd.filter(x -> !x.contains(header));
	}
	
	void observe(boolean bool){
		while(bool) {
			
		}
	}
}
