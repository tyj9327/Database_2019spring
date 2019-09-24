package myDBMSTools;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.io.File;
import java.io.UnsupportedEncodingException;
import java.util.*;


public class Table implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 201212506L;

	private String tableName = null;
	private LinkedHashMap<String, Column> columns = null;
	private ArrayList<String> columnNames = null; // ColumnNames
	private ArrayList<String> pKey = null; // can have multiple pkeys
	private LinkedHashMap<String, ArrayList<String>> referencing = null;
	private LinkedHashMap<String, ArrayList<String>> referenced = null;
	private LinkedHashMap<String, ArrayList<String>> thisTablefCol = null;
	private ArrayList<String> referingTables = null;
	private ArrayList<String> referencedBy = null;
	private ArrayList<String> fColumns = null;
	private LinkedHashMap<String, String> foreignRelation = null;
	private HashMap<String, String> findForeignColumn = null;
	private HashMap<String, String> findForeignTable = null;
	private HashMap<String, String> findHere_fromThatColumn = null;

	
	// primary key column 은 무조건 not null
	// null값을 가질 수 있는 column은 절대 primary key nono
	// Foreign key는 반드시 다른 table의 primary key
	
	public Table(String tableName, StringSplit ss) {
		this.thisTablefCol = new LinkedHashMap<String, ArrayList<String>>();
		this.thisTablefCol = ss.getForeignHash();
		this.tableName = tableName;
		this.columnNames = new ArrayList<String>();
		this.columns = new LinkedHashMap<String, Column>();
		this.pKey = new ArrayList<String>();
		this.pKey = ss.getPrimaryList();
		this.referencing = new LinkedHashMap<String, ArrayList<String>>();
		this.referencing = ss.getReferencingHash();
		
		this.referenced = new LinkedHashMap<String, ArrayList<String>>();
		
		this.referingTables = new ArrayList<String>();
		this.referingTables = ss.getReferingTable();
		this.fColumns = ss.getForeignKeys();
		this.foreignRelation = new LinkedHashMap<String, String>();
		this.foreignRelation = ss.getForeignRelation();
		this.referencedBy = new ArrayList<String>();
		this.findForeignColumn = new HashMap<String, String>();
		this.findForeignColumn = ss.findForeignColumn();
		this.findForeignTable = new HashMap<String, String>();
		this.findForeignTable = ss.findForeignTable();
		
		this.findHere_fromThatColumn = ss.findHereColumn_byThatColumn();
		
		for(int i = 0; i < ss.getCNameList().size(); i++) {
			
			Column newColumn = null;
			
			if(this.pKey.contains(ss.getCNameList().get(i))) {
				newColumn = new Column(ss.getCNameList().get(i), "N", ss.getType().get(i));
			}
			else {
				newColumn = new Column(ss.getCNameList().get(i), ss.getNotNullList().get(i), ss.getType().get(i));
			}
			this.columns.put(ss.getCNameList().get(i), newColumn);
			this.columnNames.add(ss.getCNameList().get(i));
		}
	}
	
	public LinkedHashMap<String, ArrayList<String>> getFHash(){
		return this.thisTablefCol;
	}
	
	// 자신을 참조하는 테이블들의 목록을 갱신. 
	// 자신을 참조하는 테이블을 drop하고자 하면 toDelete = true, 추가하려면 false
	public void reviseReferencedByList(boolean toDelete, String tableName) {
		if(toDelete) {
			if(referencedBy.size() > 0) {
				int count = 0;
				for(int i = 0; i < this.referencedBy.size(); i++) {
					if(referencedBy.get(i).equals(tableName))
						break;
					count++;
				}
				this.referencedBy.remove(count);			
			}
		}else {
			this.referencedBy.add(tableName);
		}
	}
	
	//public Tuple(String tableName, ArrayList<String> columnNames, ArrayList<ArrayList<String>> tuples) {
//	public void setTuple(String tableName, )
//	
	public ArrayList<String> getDataTypeRow(){
		ArrayList<String> dataTypeList = new ArrayList<String>();
		for(String column : this.columnNames) {
			dataTypeList.add(this.getColumns().get(column).getType());
		}
		return dataTypeList;
	}
	
	public String getThisTableColumn(String referingTableColumn){
		return this.foreignRelation.get(referingTableColumn);
	}
	
	public ArrayList<String> getFColumns(){
		return this.fColumns;
	}
	
	public void addReferenced(String tableName, ArrayList<String> referencedColumns) {

		this.referenced.put(tableName, referencedColumns);
	}
	
	public int referedByHowMany() {
		return this.referencedBy.size();
	}
	
	public ArrayList<String> getReferingTable(){
		return this.referingTables;
	}

	
	public void deleteColumns(String deleteColumn) {
		this.columns.remove(deleteColumn);
	}
	
	public void rename(String newName) {
		this.tableName = newName;
	}
	
	public ArrayList<String> getPrimaryKey() {
		return this.pKey;
	}
	
	public LinkedHashMap<String, Column> getColumns(){
		return this.columns;
	}
	
	public ArrayList<String> getColumnNames(){
		return this.columnNames;
	}

	public ArrayList<String> getPrimary(){
		return this.pKey;
	}
	
		
	public LinkedHashMap<String, ArrayList<String>> getReferencing() {
		return this.referencing;
	}
	
		
	public String getTableName() {
		return this.tableName;
	}
	
	public String findForeignColumn(String columnName) {
		return this.findForeignColumn.get(columnName);
	}
	
	public String findForeignTable(String columnName) {
		return this.findForeignTable.get(columnName);
	}
	
	public LinkedHashMap<String, ArrayList<String>> getThisTable_fCol(){
		return this.thisTablefCol;
	}
	
	// 참조하는 테이블 이름 - 참조하는 칼럼의 저쪽  이름 
	public void putReferencing_That(String thatTableName, ArrayList<String> thatcolumnNames) {
		this.referencing.put(thatTableName, thatcolumnNames);
	}
	// 참조하는 테이블 이름 - 참조하는 칼럼의 이쪽  이름 
	public void putReferencing_This(String thatTableName, ArrayList<String> thisColumnNames) {
		this.thisTablefCol.put(thatTableName, thisColumnNames);
	}
	
	// 나를 참조하는 테이블과 참조되는 내 테이블에서의 칼럼 이름 
	public void putReferenced(String tableName, ArrayList<String> ref) {

		this.referenced.put(tableName, ref);
		for(String cName : ref) {
			this.getColumns().get(cName).addReferingTable(tableName);
		}
	}
	
	public void deleteReferenced(String tableName) {
		ArrayList<String> ref = this.referenced.get(tableName);
		this.referenced.remove(tableName);
		for(String cName : ref) {
			this.getColumns().get(cName).removeReferingTable(tableName);
		}
	}
	
	public LinkedHashMap<String, ArrayList<String>> getReferenced() {
		return this.referenced;
	}
	
	public HashMap<String, String> getHere_fromThatCol(){
		return this.findHere_fromThatColumn;
	}
	
}

