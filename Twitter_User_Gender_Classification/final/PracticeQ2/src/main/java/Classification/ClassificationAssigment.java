package Classification;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.DecisionTreeClassifier;
import org.apache.spark.ml.feature.HashingTF;
import org.apache.spark.ml.feature.IDF;
import org.apache.spark.ml.feature.IndexToString;
import org.apache.spark.ml.feature.StopWordsRemover;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.StringIndexerModel;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.ml.classification.RandomForestClassifier;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import static org.apache.spark.sql.functions.callUDF;



public class ClassificationAssigment {
	
	private static UDF1 removeSpecialCharacters = new UDF1<String, String>() {
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		public String call(final String str) throws Exception {
			return str.replaceAll("[^a-zA-Z0-9]", " ");
		}
	};
	
	
public static void main(String args[]) {
			
		Logger.getLogger("org").setLevel(Level.ERROR);
		Logger.getLogger("akka").setLevel(Level.ERROR);

        SparkSession sparkSession = SparkSession.builder()  //SparkSession  
                .appName("SparkML") 
                .master("local[*]") 
                .getOrCreate(); //
  
        /*StructType schema = new StructType()
        	    .add("_unit_id", "string")
        	    .add("_golden", "string");*/
        
	//read the file as data
        String pathTrain = "data/gender-classifier.csv";	
        Dataset<Row> csv = sparkSession.read().option("header", true)
        		.option("mode", "DROPMALFORMED")
        		.option("inferSchema", true)
        		.option("ignoreLeadingWhiteSpace",true)
				.option("ignoreTrailingWhiteSpace",true)
        		.csv(pathTrain);
     
        String[] dropCol = {"_unit_id","_unit_state","_trusted_judgments","_last_judgment_at","gender:confidence","profile_yn"
        		,"profile_yn:confidence","created","name",
				"profile_yn_gold","profileimage","retweet_count",
				"tweet_coord","tweet_count","tweet_created","tweet_id",
				"tweet_location","user_timezone","fav_number","gender_gold","_golden","link_color","sidebar_color","description"};
		for(String column: dropCol) {
			csv = csv.drop(column);
		}
		// drop the rows containing any null or NaN values
				csv = csv.na().drop();       
				System.out.println("Dropping Rows containing null csv: "+csv.columns().length+" : "+csv.count());
				csv.show(10);
		csv = csv.filter(csv.col("gender").notEqual("unknown"));
		csv = csv.filter(csv.col("gender").notEqual(""));
		csv = csv.filter(csv.col("gender").isNotNull());
				
		//remove unwanted characters from description and text columns
				sparkSession.udf().register("removeChar", removeSpecialCharacters, DataTypes.StringType);
				//csv = csv.withColumn("description1", callUDF("removeChar", csv.col("description"))).drop("description");
				csv = csv.withColumn("text1", callUDF("removeChar", csv.col("text"))).drop("text");
				
				csv = csv.filter(csv.col("text1").notEqual(""));
				csv = csv.filter(csv.col("text1").isNotNull());
				
				/*csv = csv.filter(csv.col("description1").notEqual(""));
				csv = csv.filter(csv.col("description1").isNotNull());*/
				
				System.out.println("After splitting csv: "+csv.columns().length+" : "+csv.count());
				csv.show(10);
				
				/*VectorAssembler assembler = new VectorAssembler()
						  .setInputCols(new String[]{"description1", "text1"})
						  .setOutputCol("features_1");
				
				 Dataset<Row> newDs = assembler.transform(csv);*/
				
				//Relabel the target variable
				StringIndexerModel labelindexer = new StringIndexer()
						.setInputCol("gender")
						.setOutputCol("label").fit(csv);
				
				//performing splits on the dataset			
				Dataset<Row>[] splits = csv.randomSplit(new double[]{0.7, 0.3});
		        Dataset<Row> trainset = splits[0];		//Training Set
		        Dataset<Row> testset = splits[1];			//Testing Set	
				
				// Tokenize the input text
				Tokenizer tokenizer = new Tokenizer()
						.setInputCol("text1")
						.setOutputCol("words");

				// Remove the stop words
				StopWordsRemover remover = new StopWordsRemover()
						.setInputCol(tokenizer.getOutputCol())
						.setOutputCol("filtered");		

				// Create the Term Frequency Matrix
				HashingTF hashingTF = new HashingTF()
						.setNumFeatures(1000)
						.setInputCol(remover.getOutputCol())
						.setOutputCol("numFeatures");

				// Calculate the Inverse Document Frequency 
				IDF idf = new IDF()
						.setInputCol(hashingTF.getOutputCol())
						.setOutputCol("features");
				
				// Set up the Random Forest Model
				RandomForestClassifier rf = new RandomForestClassifier();
				
				//Set up Decision Tree
				DecisionTreeClassifier dt = new DecisionTreeClassifier();
						
						IndexToString labelConverter = new IndexToString()
								.setInputCol("prediction")
								.setOutputCol("predictedLabel").setLabels(labelindexer.labels());
				
				// Create and Run Random Forest Pipeline
				Pipeline pipelineRF = new Pipeline()
				  .setStages(new PipelineStage[] {labelindexer, tokenizer, remover, hashingTF, idf, rf, labelConverter});	
				// Fit the pipeline to training documents.
				PipelineModel modelRF = pipelineRF.fit(trainset);	
				// Make predictions on train data
				Dataset<Row> predictionsTrainRF = modelRF.transform(trainset);

				// Make predictions on test documents.
				Dataset<Row> predictionsTestRF = modelRF.transform(testset);
				System.out.println("Predictions from Random Forest Model are:");
				predictionsTestRF.show(10);

				// Create and Run Decision Tree Pipeline
				Pipeline pipelineDT = new Pipeline()
				.setStages(new PipelineStage[] {labelindexer, tokenizer, remover, hashingTF, idf, dt, labelConverter});	
				// Fit the pipeline to training documents.
				PipelineModel modelDT = pipelineDT.fit(trainset);	
				// Make predictions on train data
				Dataset<Row> predictionsTrainDT = modelDT.transform(trainset);
				
				// Make predictions on test documents.
				Dataset<Row> predictionsTestDT = modelDT.transform(testset);
				System.out.println("Predictions from Decision Tree Model are:");
				predictionsTestDT.show(10);		

				// Select (prediction, true label) and compute test error.
				MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator()
				  .setLabelCol("label")
				  .setPredictionCol("prediction")
				  .setMetricName("accuracy")
				  .setMetricName("weightedPrecision")
				  .setMetricName("weightedRecall")
				  .setMetricName("f1");
				
				//Evaluate Random Forest
				double accuracyTrainRF = evaluator.evaluate(predictionsTrainRF);
				double weightedPrecisionTrainRF = evaluator.evaluate(predictionsTrainRF);
				double weightedRecallTrainRF = evaluator.evaluate(predictionsTrainRF);
				double f1TrainRF = evaluator.evaluate(predictionsTrainRF);
				System.out.println("Random Forest Training Accuracy = " + Math.round(accuracyTrainRF * 100) + " %");
				System.out.println("Random Forest Training weightedPrecision = " + Math.round(weightedPrecisionTrainRF));
				System.out.println("Random Forest Training weightedRecall = " + Math.round(weightedRecallTrainRF));
				System.out.println("Random Forest Training f1 = " + Math.round(f1TrainRF));
				//System.out.println("Random Forest Training Error = " + (1.0 - accuracyTrainRF));
				
				double accuracyTestRF = evaluator.evaluate(predictionsTestRF);
				double weightedPrecisionTestRF = evaluator.evaluate(predictionsTestRF);
				double weightedRecallTestRF = evaluator.evaluate(predictionsTestRF);
				double f1TestRF = evaluator.evaluate(predictionsTestRF);
				System.out.println("Random Forest Test Accuracy = " + Math.round(accuracyTestRF * 100) + " %");
				System.out.println("Random Forest Test weightedPrecision = " + Math.round(weightedPrecisionTestRF));
				System.out.println("Random Forest Test weightedRecall = " + Math.round(weightedRecallTestRF));
				System.out.println("Random Forest Test f1 = " + Math.round(f1TestRF));
				//System.out.println("Random Forest Test Error = " + (1.0 - accuracyTestRF));
				
				//Evaluate Decision Tree
				double accuracyTrainDT = evaluator.evaluate(predictionsTrainDT);
				double weightedPrecisionTrainDT = evaluator.evaluate(predictionsTrainDT);
				double weightedRecallTrainDT = evaluator.evaluate(predictionsTrainDT);
				double f1TrainDT = evaluator.evaluate(predictionsTrainDT);
				System.out.println("Decision Tree Training Accuracy = " + Math.round(accuracyTrainDT * 100) + " %");
				System.out.println("Decision Tree Training weightedPrecision = " + Math.round(weightedPrecisionTrainDT));
				System.out.println("Decision Tree Training weightedRecall = " + Math.round(weightedRecallTrainDT));
				System.out.println("Decision Tree Training f1 = " + Math.round(f1TrainDT));
				//System.out.println("Decision Tree Training Error = " + (1.0 - accuracyTrainDT));
				
				double accuracyTestDT = evaluator.evaluate(predictionsTestDT);
				double weightedPrecisionTestDT = evaluator.evaluate(predictionsTestDT);
				double weightedRecallTestDT = evaluator.evaluate(predictionsTestDT);
				double f1TestDT = evaluator.evaluate(predictionsTestDT);
				System.out.println("Decision Tree Test Accuracy = " + Math.round(accuracyTestDT * 100) + " %");
				System.out.println("Decision Tree Training weightedPrecision = " + Math.round(weightedPrecisionTestDT));
				System.out.println("Decision Tree Training weightedRecall = " + Math.round(weightedRecallTestDT));
				System.out.println("Decision Tree Training f1 = " + Math.round(f1TestDT));
				//System.out.println("Decision Tree Test Error = " + (1.0 - accuracyTestDT));
	}

}
