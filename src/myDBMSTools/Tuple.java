package myDBMSTools;
import java.util.HashMap;
import java.io.Serializable;
import java.util.ArrayList;

public class Tuple implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 201212506L;
	private String tableName = null;
	private ArrayList<String> primaryValues = null;
	// columnNames의 순서와 동일한 위치에 해당 column의 튜플들이 삽입됨.
//	private HashMap<String, Integer> columnNames = null;
	private ArrayList<String> columnNames;
	
	/*
	 * <<col1, col2, col3 ... ><col1, ....> ... <col1, col2, ... coln>>
	 * */
	private ArrayList<ArrayList<String>> tuples = null;
	
	/*
	 * 테이블마다 하나의 Tuple 객체를 가지게 됨!
	 * 삽입과 삭제시 해당하는 columnName의 위치와 동일한 위치의 tuples에서 수정이 이루어짐!
	 */
	
	public Tuple(String tableName, ArrayList<String> columnNames, ArrayList<ArrayList<String>> tuples, String pValues) {
		this.tableName = tableName;
		this.tuples = new ArrayList<ArrayList<String>>();
		this.tuples = tuples;
		this.columnNames = columnNames;
//		this.columnNames = new HashMap<String, Integer>();
//		int i = 0;
//		for(String column : columnNames) {
//			this.columnNames.put(column, i++);
//		}
		this.primaryValues = new ArrayList<String>();
		this.primaryValues.add(pValues);
	}
	
	public void insertRows(ArrayList<String> row, String primaryValues) {
		this.tuples.add(row);
		this.primaryValues.add(primaryValues); // @pVal@pVal@pVal ... 의 형태로 저장!
		
	}
	
	public ArrayList<String> getPValues(){
		return this.primaryValues;
	}
	
	// column 이름과 키가 주어졌을 때 그것과 일치하는 모든 row를 tuples에서 찾아서 반환 
	// 이 메서드를 조합해서 where 에서 활용 
	public ArrayList<ArrayList<String>> getRows(String column, String key) {
		ArrayList<Integer> foundRowsIndex = new ArrayList<Integer>();	
		int columnIndex = this.columnNames.indexOf(column);
	
		for(int i = 0; i < this.tuples.size(); i++) {
			if(tuples.get(i).get(columnIndex).equals(key)) {
				foundRowsIndex.add(i);
			}
		}
		
		ArrayList<ArrayList<String>> foundRows = new ArrayList<ArrayList<String>>();
		for(int i : foundRowsIndex) {
			foundRows.add(this.tuples.get(i));
		}

		return foundRows;
	}
	
	public ArrayList<String> getColumn(String column){
		int columnIndex = this.columnNames.indexOf(column);
		ArrayList<String> columnValues = new ArrayList<String>();
		
		for(ArrayList<String> row : this.tuples) {
			if(!columnValues.contains(row.get(columnIndex))) // 뽑아낸 column은 중복되는 value를 허용하지 않음 
				columnValues.add(row.get(columnIndex));
		}
		return columnValues;
	}
	
	public ArrayList<String> getDuplicatableColumn(String column){
		int columnIndex = this.columnNames.indexOf(column);
		ArrayList<String> columnValues = new ArrayList<String>();
		
		for(ArrayList<String> row : this.tuples) {
			columnValues.add(row.get(columnIndex));
		}
		return columnValues;
	}
	
	// primary key 중복 확인할 때 활용 가능 
	// <<1, 2, 3>, <2, 3, 4>, <3, 4, 5> .... >
	public ArrayList<ArrayList<String>> getMultipleColumns(ArrayList<String> columns){
		ArrayList<ArrayList<String>> foundColumns = new ArrayList<ArrayList<String>>();
		ArrayList<Integer> columnIndices = new ArrayList<Integer>();
		
		for(String column : columns) {
			columnIndices.add(this.columnNames.indexOf(column));
		}
		
		for(int i = 0; i < this.tuples.size(); i++) {
			ArrayList<String> addingRow = new ArrayList<String>();
			for(int j : columnIndices) {
				addingRow.add(this.tuples.get(i).get(j));
			}
			foundColumns.add(addingRow);
		}
		
		return foundColumns;
	}
	
	
	
	public void deleteRow(String column, String key) {

		int columnIndex = this.columnNames.indexOf(column);
		
		for(int i = 0; i < this.tuples.size();i++) {
			if(this.tuples.get(i).get(columnIndex).equals(key)) {
				this.tuples.remove(i);
				this.primaryValues.remove(i);
				i--;
			}
		}
	}
	
	public void deleteMatchinRows(ArrayList<ArrayList<String>> chosenRows) {

		for(ArrayList<String> chosenRow : chosenRows) {
			for(ArrayList<String> originalRow : tuples) {
				if(chosenRow.equals(originalRow)) {
					int index = tuples.indexOf(originalRow);
					tuples.remove(index);
					break;
				}
			}
		}
	}
	
	public String getTableName() {
		return this.tableName;
	}
	
	public ArrayList<ArrayList<String>> getAllRows(){
		return this.tuples;
	}
	
	public void nullifyColumn(String columnName) {
		int index = columnNames.indexOf(columnName);
		for(ArrayList<String> row: this.tuples) {
			row.remove(index);
			row.add(index, "null");
		}
	}
	
	public void nullifyMatchingColumn(String columnName, ArrayList<String> values) {
		int index = columnNames.indexOf(columnName);
		for(ArrayList<String> row : this.tuples) {
			for(String value : values) {
				if(row.get(index).equals(value)) {
					row.remove(index);
					row.add(index, "null");
				}
			}
		}
	}
	
}