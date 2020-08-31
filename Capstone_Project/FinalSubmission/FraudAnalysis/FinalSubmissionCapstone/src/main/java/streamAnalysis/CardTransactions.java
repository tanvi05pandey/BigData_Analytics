package streamAnalysis;

import java.io.IOException;
import java.io.Serializable;
import java.util.UUID;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class CardTransactions implements Serializable{
	private static final long serialVersionUID = 1L;
	public static void updateCardTxns(CardTxnsPojo cardtxnsdata) throws IOException 
	{

		System.out.println("************Card Transactions table*************");

	Admin hBaseAdmin1 = HbaseConnection.getHbaseAdmin();

	Table table = null;
	CardTransactions ct = null;

	try 
	{
		Connection connection = ConnectionFactory.createConnection(hBaseAdmin1.getConfiguration());
		
		table = connection.getTable(TableName.valueOf("card_transactions_hive_new"));
		String uuid = UUID.randomUUID().toString();
		Put p = new Put(Bytes.toBytes(uuid));
		
		String latestCardId = cardtxnsdata.getCardId();
		String latestMemberId = cardtxnsdata.getMemberId();
		int latestAmount = cardtxnsdata.getAmount();
		String latestPostCode = cardtxnsdata.getPostcode();
		String latestTxn_dt = cardtxnsdata.getTransactionDt();
		String latestPosId = cardtxnsdata.getPosId();
		
		p.addColumn(Bytes.toBytes("cardID"),Bytes.toBytes("card_id"),Bytes.toBytes(latestCardId));
		p.addColumn(Bytes.toBytes("memberID"),Bytes.toBytes("member_id"),Bytes.toBytes(latestMemberId));
		p.addColumn(Bytes.toBytes("amount"),Bytes.toBytes("amount"),Bytes.toBytes(latestAmount));
		p.addColumn(Bytes.toBytes("posID"),Bytes.toBytes("pos_id"),Bytes.toBytes(latestPosId));
		p.addColumn(Bytes.toBytes("postcode"),Bytes.toBytes("postcode"),Bytes.toBytes(latestPostCode));
		p.addColumn(Bytes.toBytes("transactionDT"),Bytes.toBytes("transaction_dt"),Bytes.toBytes(latestTxn_dt));
		p.addColumn(Bytes.toBytes("status "),Bytes.toBytes("status"),Bytes.toBytes(latestTxn_dt));
		table.put(p);
		
		
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



	}
}
