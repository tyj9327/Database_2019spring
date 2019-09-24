package parsingTools;

import java.util.ArrayList;

public class WhereClause {
	private ArrayList<ArrayList<String>> chosenRows;
//	private ArrayList<ArrayList<String>> joinedRows;
	private ArrayList<String> columnNames;
	
	public WhereClause(ArrayList<ArrayList<String>> chosenRows, ArrayList<String> columnNames) {
		this.chosenRows = chosenRows;
		this.columnNames = columnNames;
//		this.joinedRows = joinedRows;
		
	}
	
	public ArrayList<String> getColumnNames(){

		return this.columnNames;
	}
	
	public ArrayList<ArrayList<String>> getChosenRows(){
		return this.chosenRows;
	}
	
//	public ArrayList<ArrayList<String>> getJoinedRows(){
//		return this.joinedRows;
//	}
}
