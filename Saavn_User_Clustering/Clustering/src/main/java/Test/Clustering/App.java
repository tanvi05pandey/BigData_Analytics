package Test.Clustering;

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

public class App {
	
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

		/*Dataset<Row> rawDataset = spark.read().option("header", "false")
                      .text("s3a://bigdataanalyticsupgrad/notification_clicks/*")
                      .select();*/
		
		Dataset<Row> rawDatasetUserClickStream = spark.read().format("csv").option("inferschema", "false")
				.load("s3a://bigdataanalyticsupgrad/activity/sample100mb.csv").toDF("user_id","timestamp","song_id","date");
		
		/*Dataset<Row> rawMetadata = spark.read().format("csv").option("inferschema", "false")
				.load("s3a://bigdataanalyticsupgrad/newmetadata/*").toDF("songId","artist_id");*/
		
		// Ignore rows having null values
		Dataset<Row> datasetUserClickStreamNew = rawDatasetUserClickStream.drop("timestamp","date");
		Dataset<Row> datasetUserClickStreamClean = datasetUserClickStreamNew.na().drop();
		
		Dataset<Row> datasetFreq = datasetUserClickStreamClean.groupBy("user_id", "song_id")
				.agg(functions.count("*").alias("frequency"));
		
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
	
	    // Build the recommendation model using ALS on the training data
	    ALS als = new ALS()
	      .setMaxIter(5)
	      .setRegParam(0.01)
	      .setImplicitPrefs(false)
	      .setUserCol("userId")
	      .setItemCol("songId")
	      .setRatingCol("frequency");
	    ALSModel model = als.fit(datasetFreq);
	    
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
					.setOutputCol("userId_orig").setLabels(indexer1.labels());
	 		
	 		Dataset<Row> finalPredictedData = labelConverter.transform(predictions).drop("userIdNum","features").dropDuplicates("userId_orig");
	 		//finalPredictedData.show(10);
	 		
	 		//join this dataset with SongId data on userId column
	 		Dataset<Row> joinedUserWithMetaData = finalPredictedData.join(datasetUserClickStreamClean, finalPredictedData.col("userId_orig")
	 				.equalTo(datasetUserClickStreamClean.col("user_id")), "inner").drop("userId_orig");
	 		
	 		joinedUserWithMetaData.show();
	 		
	 		/*Dataset<Row> groupedUsersByArtist = joinedUserWithMetaData.join(rawMetadata, joinedUserWithMetaData.col("song_id")
	 				.equalTo(rawMetadata.col("songId")),"inner").drop("songId").dropDuplicates();*/
	 		/*groupedUsersByArtist.toDF().write().format("parquet").save("file:///C://Users//tanvi//Desktop//Saavn_course6//parquet");*/
	 		
		spark.stop();
	}
}	