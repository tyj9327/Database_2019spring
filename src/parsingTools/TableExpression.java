package parsingTools;

import java.util.ArrayList;

public class TableExpression {
	private WhereClause WC;
	private ArrayList<String> fromList;
	
	public TableExpression(WhereClause WC, ArrayList<String> fromList) {
		this.WC = WC;
		this.fromList = fromList;
		
	}
	
	public ArrayList<String> getFromList(){
		return this.fromList;
	}
	
	public WhereClause getWC() {
		return this.WC;
	}
	
}
