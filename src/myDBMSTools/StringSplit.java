package myDBMSTools;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;

public class StringSplit implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 201212506L;
	private ArrayList<String> columnNameList = null;
	private ArrayList<String> dataTypeList = null;
	private ArrayList<String> notNullList = null;
	private ArrayList<String> foreignKeysList = null;
	private ArrayList<String> primaryKeysList = null;
	private ArrayList<String> primaryList = null;
	private ArrayList<String> foreignTable = null; //참조하고 있는 table의 이름들;
	private ArrayList<String> foreignColumnList = null; //foreign key
	private LinkedHashMap<String, ArrayList<String>> refering = null;
	private LinkedHashMap<String, ArrayList<String>> foreignHashmap = null;
	private LinkedHashMap<String, String> foreignRelation = null;
	private HashMap<String, String> findForeignTable = null;
	private HashMap<String, String> findForeignColumn = null;
	private HashMap<String, String> reverseFFC = null;

	public StringSplit(ArrayList<String> tElementList) throws Exception {
		
		tElementListSplit(tElementList);
	}
	
	public void tElementListSplit(ArrayList<String> tElementList) throws Exception {
		
		ArrayList<String> columnDefList = new ArrayList<String>();
		this.primaryKeysList = new ArrayList<String>(); // split 하기 전 
		this.foreignKeysList = new ArrayList<String>(); // split 하기 전 
		this.dataTypeList = new ArrayList<String>();
		this.notNullList = new ArrayList<String>();
		this.columnNameList = new ArrayList<String>();
		
		this.foreignTable = new ArrayList<String>(); // foreign table들의 이름 리스트 
		this.primaryList = new ArrayList<String>(); // split이후 primary key의 column name 리스트  
		this.foreignColumnList = new ArrayList<String>(); // 참조하는 테이블에서의 foreign column name 리스트 
		/*
		  		refering.put(fTableName, fColumnsThatTable);
				foreignHashmap.put(fTableName, fColumnsThisTable);
		*/
		this.refering = new LinkedHashMap<String, ArrayList<String>>();
		this.foreignHashmap = new LinkedHashMap<String, ArrayList<String>>();
		// 참조하는 테이블에서의 column 이름과 참조당하는 테이블에서의 column 이름 매칭 <참조당하는쪽, 참조하는쪽>
		this.foreignRelation = new LinkedHashMap<String, String>(); 
		this.findForeignTable = new HashMap<String, String>();
		this.findForeignColumn = new HashMap<String, String>();
		this.reverseFFC = new HashMap<String, String>();
		
		for(int i = 0; i < tElementList.size(); i++) {
			if(tElementList.get(i).charAt(1) == 'C') {
				columnDefList.add(tElementList.get(i));
			}else if(tElementList.get(i).charAt(1) == 'P') {
				if(primaryKeysList.size() > 0) {
					throw new Exception("Create table has failed: primary key definition is duplicated");	
				}
				this.primaryKeysList.add(tElementList.get(i).substring(9));
			}else {
				foreignKeysList.add(tElementList.get(i));
			}
		}
		
		//각 칼럼들의 정보 저장. 추출할 때에는 같은 index끼리 같은 column의 정보가 됨.
		String[] splitted = new String[4];
		for(int i = 0; i < columnDefList.size(); i++) {
			splitted = columnDefList.get(i).split("@");
			
			//DuplicateColumnDefError 출력;
			if(columnNameList.contains(splitted[1])) {
				throw new Exception("Create table has failed: column definition is duplicated");
			}else {
				columnNameList.add(splitted[1]);
			}
			
			//CharLengthError 출력;
			if(splitted[2].substring(0, 1).equals("c")) {
				int charLength = Integer.parseInt(splitted[2].substring(5, splitted[2].length() - 1));
				if(charLength < 1) {
					throw new Exception("Char length should be over 0");
				}
			}
			dataTypeList.add(splitted[2]);
			notNullList.add(splitted[3]);
		}
		
		// Primary key 저장;
		if(primaryKeysList.size() == 0) {
			throw new Exception("Create table has failed: primary key definition does not exist");
		}else {
			String[] pSplitted = primaryKeysList.get(0).split("@");
			for (int i = 0; i < pSplitted.length; i++) {
				if(!columnNameList.contains(pSplitted[i])) {
					throw new Exception("Create table has failed: '" + pSplitted[i] + "' does not exists in column definition");
				}else {
					this.primaryList.add(pSplitted[i]);		
				}
		
			}
		}

		
		// Foreign key 정리;
		for(int i = 0; i < foreignKeysList.size(); i++) {
			String[] fSplitted = foreignKeysList.get(i).split("@");
			boolean isReference = false;
			boolean isTableName = true;
			String fTableName = null;
			// 이 테이블의 foreign key columns
			ArrayList<String> fColumnsThisTable = new ArrayList<String>(); 
			// 참조하는 테이블의 foreign key columns (두 테이블에서 각각 칼럼의 이름이 다르므로 생성)
			ArrayList<String> fColumnsThatTable = new ArrayList<String>(); 
				
			for(int j = 1; j < fSplitted.length; j++) {
				if(fSplitted[j].equals("#Reference")) {
					isReference = true;
					j++;
				}
				if(!isReference) {
					
					if(!this.columnNameList.contains(fSplitted[j])) {
						throw new Exception("Create table has failed: '" + fSplitted[j] + "' does not exists in column definition");
					}else {

						foreignColumnList.add(fSplitted[j]);
						fColumnsThisTable.add(fSplitted[j]);
					}
				}else if(isTableName) {

					fTableName = fSplitted[j];
					foreignTable.add(fSplitted[j]);
					isTableName = false;
				}else {
					fColumnsThatTable.add(fSplitted[j]);

				}
			}
			// for문 종료 
			
			if(fColumnsThisTable.size() != fColumnsThatTable.size()) {
				throw new Exception("Create table has failed: foreign key references wrong type");
			}else {
			
				for(int k = 0; k < fColumnsThatTable.size(); k++) {
					this.foreignRelation.put(fColumnsThatTable.get(k), fColumnsThisTable.get(k));
					this.reverseFFC.put(fColumnsThatTable.get(k), fColumnsThisTable.get(k));
					this.findForeignColumn.put(fColumnsThisTable.get(k), fColumnsThatTable.get(k));
					this.findForeignTable.put(fColumnsThisTable.get(k), fTableName);
				}
				
				this.refering.put(fTableName, fColumnsThatTable);
				this.foreignHashmap.put(fTableName, fColumnsThisTable);
				
			}
		}
	}
	public HashMap<String, String> findHereColumn_byThatColumn(){
		return this.reverseFFC;
	}
	
	public HashMap<String, String> findForeignColumn() {
		return this.findForeignColumn;
	}
	
	public HashMap<String, String> findForeignTable() {
		return this.findForeignTable;
	}
	
	public LinkedHashMap<String, String> getForeignRelation(){
		return this.foreignRelation;
	}
	
	public LinkedHashMap<String, ArrayList<String>> getReferencingHash(){
		return this.refering;
	}
	
	public LinkedHashMap<String, ArrayList<String>> getForeignHash(){
		return this.foreignHashmap;
	}
	
	
	// foreign keys들 칼럼 반환;
	public ArrayList<String> getForeignKeys(){
		return this.foreignColumnList;
	}
	
	// 참조하는 table의 이름 반환;
	public ArrayList<String> getReferingTable(){
		return this.foreignTable;
	}
	
	// 칼럼들의 이름 리스트 반환;
	public ArrayList<String> getCNameList(){
		return this.columnNameList;
	}
	
	// primary key 칼럼 리스트 반환;
	public ArrayList<String> getPrimaryList(){
		return this.primaryList;
	}
	
	public ArrayList<String> getNotNullList(){
		return this.notNullList;
	}
	
//	//참조하는 테이블에서의 column name들 반환;
//	public ArrayList<String> getReferringTableColumns(){
//		return this.referingColumns;
//	}
	
	public ArrayList<String> getType(){
		return this.dataTypeList;
	}

}

