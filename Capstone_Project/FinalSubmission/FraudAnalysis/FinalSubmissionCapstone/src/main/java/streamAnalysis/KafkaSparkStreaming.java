package streamAnalysis;

import java.io.IOException;
import java.math.RoundingMode;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

public class KafkaSparkStreaming implements java.io.Serializable{


    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;
    public static Boolean flag = true;
    private static DecimalFormat df2 = new DecimalFormat("#.##");
	@SuppressWarnings("static-access")
	public static void main(String[] args) throws Exception {

        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        
        

        SparkConf sparkConf = new SparkConf().setAppName("KafkaSparkStreamingDemo").setMaster("local");

        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(1));
        
        //create a unique groupId
        Instant instant = Instant.now();
		long timeStampSeconds = instant.getEpochSecond();
        String groupIDInitial = "tanvikafkaspark";
        String groupIdWithTimeEpoch = groupIDInitial+":"+timeStampSeconds;
        
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "100.24.223.181:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", groupIdWithTimeEpoch);
        kafkaParams.put("auto.offset.reset", "earliest");
        kafkaParams.put("enable.auto.commit", true);
        Collection<String> topics = Arrays.asList("transactions-topic-verified");

        JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(jssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams));
        //System.out.println(stream);
        JavaDStream<String> jds = stream.map(x -> x.value());
        jds.foreachRDD(x -> System.out.println(x.count()));
        //System.out.println("JDS count "+jds.count());

        jds.foreachRDD(new VoidFunction<JavaRDD<String>>() {

            private static final long serialVersionUID = 1L;

            @Override
            public void call(JavaRDD<String> rdd) throws IOException, ParseException {
            	
                HbaseConnection hbaseConnection = new HbaseConnection();
                HbaseDAO hbaseDao = new HbaseDAO();
                DistanceUtility disUtil = new DistanceUtility();
                CardTransactions cardTxns = new CardTransactions();
				hbaseConnection.getHbaseAdmin();
            	System.out.println("***********Fetching Streaming Data***************");
            	//System.out.println(rdd.isEmpty());
                rdd.foreach(a -> {
                	System.out.println(a);
                	JSONParser parser = new JSONParser();
                		JSONObject jsonObject = (JSONObject) parser.parse(a);
                		//Parse the incoming data
                		String card_id = jsonObject.get("card_id").toString();
                		String member_id = jsonObject.get("member_id").toString();
                		int amount = Integer.parseInt(jsonObject.get("amount").toString());
                		String postcode = jsonObject.get("postcode").toString();
                		String pos_id = jsonObject.get("pos_id").toString();
                		String transaction_dt = jsonObject.get("transaction_dt").toString();
                		
                		System.out.println("***********Parsing finished***************");
                		
                		TransactionData transactionData = new TransactionData(card_id, member_id, amount, postcode, pos_id, transaction_dt);
                		
                		Object lookup = hbaseDao.lookupData(transactionData);
                		
                		//lookup.toString().
                		System.out.println("***********Fetching Lookup data***************");
                		//System.out.println(lookup.toString());
                		
                		int lookup_ucl = 0;
                		int lookup_score = 0;
                		String lookup_zipcode = "";
                		String lookup_transaction_dt = "";
                		
                		for (java.lang.reflect.Field field : lookup.getClass().getDeclaredFields()) {
                		    field.setAccessible(true); 
                		    Object value = field.get(lookup); 
                		    if (value != null) {
                		       // System.out.println(field.getName() + "=" + value);
                		        if(field.getName() == "ucl") {
                		        	lookup_ucl = (int) value;
                		        }
                		        if(field.getName() == "score") {
                		        	lookup_score = (int) value;
                		        }
                		        if(field.getName() == "zipcode") {
                		        	lookup_zipcode = (String) value;
                		        }
                		        if(field.getName() == "txn_dt") {
                		        	lookup_transaction_dt = (String) value;
                		        }
                		    }
                		}
                		
                		//System.out.println("***********Distance Utility***************");
                		
                		//System.out.println(disUtil.getDistanceViaZipCode(postcode, lookup_zipcode));
                		double dist = disUtil.getDistanceViaZipCode(postcode, lookup_zipcode);
                		df2.setRoundingMode(RoundingMode.DOWN);
        				double roundDist = Double.parseDouble(df2.format(dist));
                		DateFormat formatter = new SimpleDateFormat("dd-MM-yyyy HH:mm:ss");
		        		Date dt_incoming = formatter.parse(transaction_dt);
		        		Date dt_last = formatter.parse(lookup_transaction_dt);
		        		long timediffMillies = Math.abs(dt_incoming.getTime() - dt_last.getTime());
		        		int timeDiffSeconds = (int) Math.abs((timediffMillies/1000));
		        		//System.out.println("***********Time Diff***************");
		        		System.out.println(timeDiffSeconds);
		        		System.out.println("Actual Distance "+dist);
		        		System.out.println("Round Distance "+roundDist);
		        		System.out.println("Time Diff in Seconds "+timeDiffSeconds);
		        		
		        		double current_speed = roundDist/timeDiffSeconds;
		        		/*System.out.println("***********Current data***********");
		        		System.out.println("Speed: "+current_speed+" Amount: "+amount);
		        		System.out.println("***********Lookup data***********");
		        		System.out.println("UCL: "+lookup_ucl+" SCORE: "+lookup_score);*/
		        		if(current_speed >= 0.25 || amount > lookup_ucl || lookup_score < 200) {
		        			flag = false;
		        			System.out.println("FRAUD");
		        		}
		        		else {
		        			flag = true;
		        			System.out.println("GENUINE");
		        			
		        			//if transaction is genuine, update lookup table
		        			
		        			hbaseDao.updateLookupData(transactionData);
		        			
		        		}
		        		
		        		if(flag) {
		        			CardTxnsPojo cardtxnsdata = new CardTxnsPojo(card_id, member_id, amount, postcode, pos_id, transaction_dt, "GENUINE");
		        			cardTxns.updateCardTxns(cardtxnsdata);
		        		}
		        		else {
		        			CardTxnsPojo cardtxnsdata = new CardTxnsPojo(card_id, member_id, amount, postcode, pos_id, transaction_dt, "FRAUD");
		        			cardTxns.updateCardTxns(cardtxnsdata);
		        		}
                		
                	});
            }
        });

        jssc.start();
        // / Add Await Termination to respond to Ctrl+C and gracefully close Spark
        // Streams
        jssc.awaitTermination();
        //jssc.close();
        
    }

}
