package spark.udemy;

import static org.apache.spark.sql.functions.col;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class SQLPractice {

	static String appName = "Udemy-Action";
	static String master = "local";

	public static void main(String[] args) {
		Logger.getLogger("org").setLevel(Level.ERROR);
		Logger.getLogger("akka").setLevel(Level.ERROR);

		SparkConf conf = new SparkConf().setAppName(appName).setMaster(master);
		SparkSession session = SparkSession.builder().appName(appName).master(master)
				.config("spark.sql.warehouse.dir", "file:/c:/tmp/spark-warehouse").getOrCreate();
		;
		try (JavaSparkContext context = new JavaSparkContext(conf)) {
			JavaRDD<String> file = context.textFile("data/iris.csv");
			String header = file.first();
			JavaRDD<String> irisData = file.filter(x -> !x.equals(header));

			JavaRDD<Row> rows = irisData.map(new Function<String, Row>() {
				public Row call(String str) {
					String[] values = str.split(",");
					Row retRow = RowFactory.create(Float.valueOf(values[0]), Float.valueOf(values[1]),
							Float.valueOf(values[2]), Float.valueOf(values[3]), values[4]);
					return retRow;
				}
			});

			StructType schema = DataTypes.createStructType(
					new StructField[] { DataTypes.createStructField("SepalLength", DataTypes.FloatType, false),
							DataTypes.createStructField("SepalWidth", DataTypes.FloatType, false),
							DataTypes.createStructField("PetalLength", DataTypes.FloatType, false),
							DataTypes.createStructField("PetalWidth", DataTypes.FloatType, false),
							DataTypes.createStructField("Species", DataTypes.StringType, false) });

			Dataset<Row> irisDataSet = session.createDataFrame(rows, schema);
			long count = irisDataSet.filter(col("PetalWidth").gt(0.4)).count();

			System.out.println("Number of Species having PetalWidth greater than 0.4 is " + count);

			irisDataSet.createOrReplaceTempView("IRIS");
			session.sql("SELECT Species, ROUND(AVG(PetalWidth),2) as Avg_Petal_Width FROM IRIS GROUP BY (Species)")
					.show();
		}
	}
}
