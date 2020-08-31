package streamAnalysis;

public class TransactionData implements java.io.Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String card_id;
	private String member_id;
	private int amount;
	private String postcode;
	private String pos_id;
	private String transaction_dt;
	
	public TransactionData(String card_id, String member_id, int amount, String postcode, String pos_id, String transaction_dt){
		this.card_id = card_id;
		this.member_id = member_id;
		this.amount = amount;
		this.postcode = postcode;
		this.pos_id = pos_id;
		this.transaction_dt = transaction_dt;
	}
	
	public String getCardId()
	{
		return this.card_id;
	}
	public String getMemberId()
	{
		return this.member_id;
	}
	public int getAmount()
	{
		return this.amount;
	}
	public String getPostcode()
	{
		return this.postcode;
	}
	public String getPosId()
	{
		return this.pos_id;
	}
	public String getTransactionDt()
	{
		return this.transaction_dt;
	}
}
