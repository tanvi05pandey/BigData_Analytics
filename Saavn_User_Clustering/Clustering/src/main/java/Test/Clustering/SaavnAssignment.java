/*package Test.Clustering;

import static org.apache.spark.sql.functions.callUDF;

import java.io.Serializable;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.evaluation.ClusteringEvaluator;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.feature.IndexToString;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.StringIndexerModel;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.api.java.UDF1;

import scala.collection.Seq;

import org.apache.spark.ml.recommendation.ALS;
import org.apache.spark.ml.recommendation.ALSModel;
import org.apache.spark.ml.recommendation.ALS.Rating;
import org.apache.spark.rdd.RDD;

public class SaavnAssignment {
	
	private static UDF1 toVector = new UDF1<Seq<Float>, Vector>(){

		private static final long serialVersionUID = 1L;  
		public Vector call(Seq<Float> t1) throws Exception {

		    List<Float> L = scala.collection.JavaConversions.seqAsJavaList(t1);
		    double[] DoubleArray = new double[t1.length()]; 
		    for (int i = 0 ; i < L.size(); i++) { 
		      DoubleArray[i]=L.get(i); 
		    } 
		    return Vectors.dense(DoubleArray); 
		  } 
		};

	public static void main(String[] args) {
		
		Logger.getLogger("org").setLevel(Level.ERROR);
		Logger.getLogger("akka").setLevel(Level.ERROR);

		 
		// Create a SparkSession
		//SparkSession spark = SparkSession.builder().appName("KMeansCluster").master("local").getOrCreate();
		
		
		SparkSession spark = SparkSession.builder().config("fs.s3.awsAccessKeyId","AKIAJJERDJBRDN6QLQOQ")
        .config("fs.s3.awsSecretAccessKey", "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")
        .appName("clustering")
        //.master("local[*]")
        .config("spark.sql.autoBroadcastJoinThreshold", 1*1024*1024*1024)
        .config("maximizeResourceAllocation", true)
        .config("spark.shuffle.compress", true)
        .config("spark.shuffle.spill.compress", true)
        .config("spark.sql.shuffle.partitions", 10)
        .config("spark.executor.extraJavaOptions", "-XX:+UseG1GC -XX:+UnlockDiagnosticVMOptions -XX:+G1SummarizeConcMark -XX:InitiatingHeapOccupancyPercent=35 -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:OnOutOfMemoryError='kill -9 %p'")
        .config("spark.driver.extraJavaOptions", "-XX:+UseG1GC -XX:+UnlockDiagnosticVMOptions -XX:+G1SummarizeConcMark -XX:InitiatingHeapOccupancyPercent=35 -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:OnOutOfMemoryError='kill -9 %p'")
        .config("spark.dynamicAllocation.enabled", true)
        .config("spark.shuffle.service.enabled", true)
        .config("spark.scheduler.mode", "FAIR")
        .getOrCreate();

		Dataset<Row> rawDataset = spark.read().option("header", "false")
                      .text("s3a://bigdataanalyticsupgrad/notification_clicks/*")
                      .select();
		
		Dataset<Row> rawDatasetUserClickStream = spark.read().format("csv").option("inferschema", "false")
				.load("s3a://bigdataanalyticsupgrad/activity/sample100mb.csv").toDF("user_id","timestamp","song_id","date");
		
		Dataset<Row> rawMetadata = spark.read().format("csv").option("inferschema", "false")
				.load("s3a://bigdataanalyticsupgrad/newmetadata/*").toDF("song_id","artist_id");
		//rawMetadata.show();
		
		//join userclickstream data and metadata on songid
		Dataset<Row> joinedData = rawDatasetUserClickStream.join(rawDatasetUserClickStream, rawMetadata.col("song_id")
 				.equalTo(rawDatasetUserClickStream.col("song_id")), "inner");
		
		joinedData.show(10);
		
		// Ignore rows having null values
		Dataset<Row> datasetUserClickStreamNew = rawDatasetUserClickStream.drop("timestamp","date");
		Dataset<Row> datasetUserClickStreamClean = datasetUserClickStreamNew.na().drop();
		
		Dataset<Row> datasetFreq = datasetUserClickStreamClean.groupBy("user_id", "song_id")
				.agg(functions.count("*").alias("frequency"));
	//	datasetFreq.show();
				//datasetFreq.show();
		
		//datasetUserClickStreamClean.show();
		
		
		
		//convert userid, songId to numeric using stringindexer
		StringIndexerModel indexer1 = new StringIndexer()
						.setInputCol("user_id")
						.setOutputCol("userId").fit(datasetFreq);
			    
		StringIndexerModel indexer2 = new StringIndexer()
						.setInputCol("song_id")
						.setOutputCol("songId").fit(datasetFreq);
			    List<StringIndexerModel> stringIndexerModel = Arrays.asList(indexer1, indexer2);
				for(StringIndexerModel indexer:stringIndexerModel) {
					datasetFreq = indexer.transform(datasetFreq);
				}
				
				datasetFreq.drop("user_id", "song_id");
				
				datasetFreq.show();
				
				//Dataset<Row> datasetUserClickStreamCleanNew = datasetUserClickStreamClean.drop("user_id", "song_id");
				
				RDD<Row> ratingsRDD = datasetFreq.rdd();
			    Dataset<Row> ratings = spark.createDataFrame(ratingsRDD, Rating.class);
			    Dataset<Row>[] splits = ratings.randomSplit(new double[]{0.8, 0.2});
			    Dataset<Row> training = splits[0];
			    Dataset<Row> test = splits[1];
			   // training.show();
	    // Build the recommendation model using ALS on the training data
	    ALS als = new ALS()
	      .setMaxIter(5)
	      .setRegParam(0.01)
	      .setImplicitPrefs(false)
	      .setUserCol("userId")
	      .setItemCol("songId")
	      .setRatingCol("frequency");
	    ALSModel model = als.fit(datasetFreq);
	    
	 // Evaluate the model by computing the RMSE on the test data
	    // Note we set cold start strategy to 'drop' to ensure we don't get NaN evaluation metrics
	    model.setColdStartStrategy("drop");

	    Dataset<Row> alsOutput = model.userFactors();
	    
	   // alsOutput.show();
	    spark.udf().register("toVector", toVector, new VectorUDT());
	    Dataset<Row> alsOutputToVector = alsOutput.withColumn("featuresnew", callUDF("toVector", alsOutput.col("features"))).drop("features")
	    		.toDF("userIdNum", "features");
	   
	   // alsOutputToVector.show();
	  
	 // Train a k-means model
	 		KMeans kmeans = new KMeans().setK(10);
	 		KMeansModel kmeansmodel = kmeans.fit(alsOutputToVector);
	 		
	 // Make predictions
	 		Dataset<Row> predictions = kmeansmodel.transform(alsOutputToVector);
	 		
	
	 	// Convert indexed labels back to original labels once prediction is available
	 		IndexToString labelConverter = new IndexToString()
					.setInputCol("userIdNum")
					.setOutputCol("predictedLabel");
	 		
	 		IndexToString labelConverter = new IndexToString()
					.setInputCol("userIdNum")
					.setOutputCol("userId_orig").setLabels(indexer1.labels());
	 		
	 		
	 		
	 		//join this dataset with SongId data on userId column
	 		Dataset<Row> joinedData = predictions.join(predictions, datasetFreq.col("userId")
	 				.equalTo(predictions.col("userIdNum")), "inner");
	 		
	 		joinedData.show(10);
	 		
	 		Dataset<Row> finalPredictedData = labelConverter.transform(predictions).drop("userIdNum");
	 		finalPredictedData.show(10);
	 		
	 		
	 				//.drop(finalPredictedData.col("col3"));
	   // Dataset<Row> predictions = model.transform(test);
	    RegressionEvaluator evaluator = new RegressionEvaluator()
	    	      .setMetricName("rmse")
	    	      .setLabelCol("rating")
	    	      .setPredictionCol("prediction");
	    	  //  double rmse = evaluator.evaluate(predictions);
	    	   // System.out.println("Root-mean-square error = " + rmse);
	 // Generate top 10 movie recommendations for each user
	  //  Dataset<Row> userRecs = model.recommendForAllUsers(10);
	    // Generate top 10 user recommendations for each movie
	   // Dataset<Row> movieRecs = model.recommendForAllItems(10);
	    
	   // userRecs.show();
	   // movieRecs.show();

		// Adding Total Price Column
		// Total Price = Quantity * UnitPrice
		Column totalPrice = datasetClean.col("Quantity").multiply(datasetClean.col("UnitPrice"));
		Dataset<Row> datasetTotPrice = datasetClean.withColumn("Total_Price", totalPrice);
		datasetTotPrice.show();
		
		// Convert all timestamps into dd/MM/yy HH:mm format 01/12/10 8:26:00 AM
		// Calculate RFM attributes : Recency, Frequency and Monetary Values
		Dataset<Row> datasetRetail = datasetTotPrice.withColumn("DaysBefore", functions.datediff(
				functions.current_timestamp(),
				functions.unix_timestamp(datasetTotPrice.col("InvoiceDate"), "MM/dd/yyyy HH:mm").cast("timestamp")));
		datasetRetail.show();

		// Recency
		Dataset<Row> datasetRecency = datasetRetail.groupBy("CustomerID")
				.agg(functions.min("DaysBefore").alias("Recency"));
		datasetRecency.show();
		
		// Frequency
		Dataset<Row> datasetFreq = datasetRetail.groupBy("CustomerID", "InvoiceNo").count().groupBy("CustomerID")
				.agg(functions.count("*").alias("Frequency"));
		datasetFreq.show();
		
		// Monetary
		Dataset<Row> datasetMon = datasetRetail.groupBy("CustomerID")
				.agg(functions.round(functions.sum("Total_Price"), 2).alias("Monetary"));
		datasetMon.show();

		Dataset<Row> datasetMf = datasetMon
				.join(datasetFreq, datasetMon.col("CustomerID").equalTo(datasetFreq.col("CustomerID")), "inner")
				.drop(datasetFreq.col("CustomerID"));
		datasetMf.show();	
		
		Dataset<Row> datasetRfm1 = datasetMf
				.join(datasetRecency, datasetRecency.col("CustomerID").equalTo(datasetMf.col("CustomerID")), "inner")
				.drop(datasetFreq.col("CustomerID"));
		datasetRfm1.show();
		
		VectorAssembler assembler = new VectorAssembler()
				  .setInputCols(new String[] {"Monetary", "Frequency", "Recency"}).setOutputCol("features");
				
		Dataset<Row> datasetRfm = assembler.transform(datasetRfm1);
		datasetRfm.show();
		 
		// Trains a k-means model
		KMeans kmeans = new KMeans().setK(3);
		KMeansModel model = kmeans.fit(datasetRfm);
		
		// Make predictions
		Dataset<Row> predictions = model.transform(datasetRfm);
		predictions.show(200);
		
		// Evaluate clustering by computing Silhouette score
		ClusteringEvaluator evaluator = new ClusteringEvaluator();

		double silhouette = evaluator.evaluate(predictions);
		System.out.println("Silhouette with squared euclidean distance = " + silhouette);

		// Shows the result
		Vector[] centers = model.clusterCenters();
		System.out.println("Cluster Centers: ");
		for (Vector center : centers) {	
			System.out.println(center);
		}
		spark.stop();
	}
}	*/