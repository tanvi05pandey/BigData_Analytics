package streamAnalysis;

import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class HbaseDAO implements Serializable{


private static final long serialVersionUID = 1L; 
private static int ucl;
private static int score;
private static String zipcode;
private static String txn_dt;

/*HbaseDAO(HbaseDAO hb) 
{ 
	hb.ucl = ucl;
	hb.score = score;
	hb.zipcode = zipcode;
	hb.txn_dt = txn_dt;
}*/

HbaseDAO(int ucl, int score, String zipcode, String txn_dt) 
{ 
	this.ucl = ucl;
	this.score = score;
	this.zipcode = zipcode;
	this.txn_dt = txn_dt;
}

public String toString() 
{ 
    return ucl + " " + score + " " + zipcode + " " + txn_dt; 
}


HbaseDAO(){}

/**

 *

 * @param transactionData

 * @return get member's score from look up HBase table.

 * @throws IOException 

 */

public static Object lookupData(TransactionData transactionData) throws IOException {
	System.out.println("DAO");

Admin hBaseAdmin1 = HbaseConnection.getHbaseAdmin();

Table table = null;

HbaseDAO hb1 = null;



try 
{
	Connection connection = ConnectionFactory.createConnection(hBaseAdmin1.getConfiguration());
	
	table = connection.getTable(TableName.valueOf("tmp_hive_lookup_table"));
	
	Get g = new Get(Bytes.toBytes(transactionData.getCardId()));

	Result result = table.get(g);
	
	byte[] ucl = result.getValue(Bytes.toBytes("card_id_and_ucl"), Bytes.toBytes("ucl"));
	byte[] score = result.getValue(Bytes.toBytes("score"), Bytes.toBytes("score"));
	byte[] zipcode = result.getValue(Bytes.toBytes("location_and_date"), Bytes.toBytes("postcode"));
	byte[] txn_dt = result.getValue(Bytes.toBytes("location_and_date"), Bytes.toBytes("transaction_dt"));
	
	
	hb1.ucl = Integer.parseInt(Bytes.toString(ucl));
	hb1.score = Integer.parseInt(Bytes.toString(score));
	hb1.zipcode = Bytes.toString(zipcode);
	hb1.txn_dt = Bytes.toString(txn_dt);
	
	//System.out.println(hb1);
	HbaseDAO hb2 = new HbaseDAO(hb1.ucl, hb1.score, hb1.zipcode, hb1.txn_dt);
	hb1 = hb2;
	//hb1.HbaseDAO(hb2);
	
	
}catch (Exception e) {

e.printStackTrace();

}
finally
{
	try {

		if (table != null)

		table.close();
		//connection.close();

		} catch (Exception e) {

		e.printStackTrace();

		}
}
return hb1;

}
public static void updateLookupData(TransactionData transactionData) throws IOException 
{

	System.out.println("Updating lookup table");

Admin hBaseAdmin1 = HbaseConnection.getHbaseAdmin();

Table table = null;

//HbaseDAO hb1 = null;



try 
{
	Connection connection = ConnectionFactory.createConnection(hBaseAdmin1.getConfiguration());
	
	table = connection.getTable(TableName.valueOf("tmp_hive_lookup_table"));
	
	Put p = new Put(Bytes.toBytes(transactionData.getCardId()));
	
	String latestPostCode = transactionData.getPostcode();
	String latestTxn_dt = transactionData.getTransactionDt();

	p.addColumn(Bytes.toBytes("location_and_date"),Bytes.toBytes("postcode"),Bytes.toBytes(latestPostCode));
	p.addColumn(Bytes.toBytes("location_and_date"),Bytes.toBytes("transaction_dt"),Bytes.toBytes(latestTxn_dt));
	table.put(p);
	
	
}catch (Exception e) {

e.printStackTrace();

}
finally
{
	try {

		if (table != null)

		table.close();

		} catch (Exception e) {

		e.printStackTrace();

		}
}



}

}
