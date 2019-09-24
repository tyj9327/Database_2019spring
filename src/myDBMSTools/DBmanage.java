package myDBMSTools;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.util.*;
import parsingTools.*;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.LockMode;

import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;

public class DBmanage implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 201212506L;
	
	
	
	public static ArrayList<ArrayList<String>> makingJoinedRows(Database tableDB, Database tupleDB, ArrayList<String> fromList) {
		ArrayList<ArrayList<String>> joinedRows = new ArrayList<ArrayList<String>>();
		
		try {
			
			ArrayList<String> renamed_TableList = new ArrayList<String>();
			ArrayList<String> original_TableList = new ArrayList<String>();
			
			for(String tName : fromList) {
				if(tName.contains("#")) {
					String[] renamed = tName.split("#");
					original_TableList.add(renamed[0]);
					renamed_TableList.add(renamed[1]);
				}else {
					original_TableList.add(tName);
					renamed_TableList.add(tName);
				}
			}
			
			// from 문에 나온 모든 table들을 우선 전체 join 시행 
			ArrayList<Tuple> selectedTuples = new ArrayList<Tuple>();	
			ArrayList<Table> selectedTables = new ArrayList<Table>();
			
			for(String tName : original_TableList) {
			
				if(DBmanage.getData(tableDB, tName) != null) {
					Table selTable = (Table) DBmanage.getData(tableDB, tName);
					selectedTables.add(selTable);
				}else {
					throw new myTableException("Selection has failed: '" + tName + "' does not exist");
				}
				
				if(DBmanage.getTupleData(tupleDB, tName) != null) {
					Tuple selTuple = (Tuple) DBmanage.getTupleData(tupleDB, tName);
					selectedTuples.add(selTuple);
				// 튜플이 존재하지 않으면 오류 !
				}else {
					throw new myTableException("Table " + tName + " has no value");
				}
			}
			
			ArrayList<ArrayList<String>> firstTuple = selectedTuples.get(0).getAllRows();
			// rows after joining every table in from clause & columnNames after joining
	
			if(selectedTuples.size() > 1) {
				joinedRows.addAll(multipleJoin(selectedTuples, firstTuple));
			}else {
				joinedRows.addAll(firstTuple);
			}

		}catch(myTableException mte) {
			mte.printStackTrace();
		}catch(Exception e) {
			e.printStackTrace();
		}
		
		return joinedRows;
	}

	
	// <tName.cName ... > 으로 반환하거나 혹은 <cName, cName ...>으로 반환 
	public static ArrayList<String> getColumnNameList(Database tableDB, ArrayList<String> fromList){
		
		ArrayList<String> columnNameList = new ArrayList<String>();
		
		try {
			
			ArrayList<String> renamed_TableList = new ArrayList<String>();
			ArrayList<String> original_TableList = new ArrayList<String>();
			
			for(String tName : fromList) {
				if(tName.contains("#")) {
					String[] renamed = tName.split("#");
					original_TableList.add(renamed[0]);
					renamed_TableList.add(renamed[1]);
				}else {
					original_TableList.add(tName);
					renamed_TableList.add(tName);
				}
			}
			
			ArrayList<Table> selectedTables = new ArrayList<Table>();
			// DB에서 해당 테이블이 존재하는지 확인 후 있으면 selectedTables list에 추가 
			for(String tName : original_TableList) {
				
				if(DBmanage.getData(tableDB, tName) != null) {
					Table selTable = (Table) DBmanage.getData(tableDB, tName);
					selectedTables.add(selTable);
				}else {
					throw new myTableException("Selection has failed: '" + tName + "' does not exist");
				}	
			}
			
//			// 만약 from절에 테이블을 하나만 적었다면 칼럼 이름을 그냥 <cName, cName ...>으로 저장 
//			if(selectedTables.size() == 1) {
//				
//				columnNameList.addAll(selectedTables.get(0).getColumnNames());
//			
//			// 만약 from절에 테이블이 여러개라면, 칼럼 이름을 <tName.cName, tName.cName ... > 으로 저장 
//			}else {
				
			for(int i = 0; i < selectedTables.size(); i++) {
				String renamedTableName = renamed_TableList.get(i);
				for(String cName : selectedTables.get(i).getColumnNames()) {
					columnNameList.add(renamedTableName + "." + cName);
				}
//				}

			}
			
		}catch(myTableException mte) {
			mte.printStackTrace();
		}catch(Exception e) {
			e.printStackTrace();
		}
		
		return columnNameList;
		
	}
	
	
	public static ArrayList<String> getDataTypeList(Database tableDB, ArrayList<String> fromList){
		
		ArrayList<String> dataTypeList = new ArrayList<String>();
		
		try {
			ArrayList<String> renamed_TableList = new ArrayList<String>();
			ArrayList<String> original_TableList = new ArrayList<String>();
			
			for(String tName : fromList) {
				if(tName.contains("#")) {
					String[] renamed = tName.split("#");
					original_TableList.add(renamed[0]);
					renamed_TableList.add(renamed[1]);
				}else {
					original_TableList.add(tName);
					renamed_TableList.add(tName);
				}
			}
			
			ArrayList<Table> selectedTables = new ArrayList<Table>();
			// DB에서 해당 테이블이 존재하는지 확인 후 있으면 selectedTables list에 추가 
			for(String tName : original_TableList) {
				
				if(DBmanage.getData(tableDB, tName) != null) {
					Table selTable = (Table) DBmanage.getData(tableDB, tName);
					selectedTables.add(selTable);
				}else {
					throw new myTableException("Selection has failed: '" + tName + "' does not exist");
				}	
			}
			
			for(Table selTable : selectedTables) {
				
				for(String cName : selTable.getColumnNames()) {
					String dt = selTable.getColumns().get(cName).getType();
					if(dt.contains("char")) {
						dataTypeList.add(dt.substring(0, 4));
					}else {
						dataTypeList.add(dt);
					}
				}
			}
			
			
		}catch(myTableException mte) {
			mte.printStackTrace();
		}catch(Exception e) {
			e.printStackTrace();
		}
		
		return dataTypeList;
		
		
	}
	

	//selectTableDB(tDB, tupleDB, finalChosenRows, selList);
	
	public static void selectTableDB(Database tableDB, Database tupleDB, TableExpression TE, ArrayList<String> selectList) throws myTableException, Exception{
		boolean isAsterisk;
		if(selectList.get(0).equals("*")) {
			isAsterisk = true;
		}else {
			isAsterisk = false;
		}
		

		ArrayList<ArrayList<String>> chosenRows;
		ArrayList<String> columnNames;
		
		if(TE.getWC() == null) {
			chosenRows = DBmanage.makingJoinedRows(tableDB, tupleDB, TE.getFromList()); // 전부 join한 tuple들 
			columnNames = DBmanage.getColumnNameList(tableDB, TE.getFromList());

		}else {
			chosenRows = TE.getWC().getChosenRows();
			columnNames = TE.getWC().getColumnNames();
		}
		
		// select문에서 rename하면 저장 
		ArrayList<String> renamed_cNameList = new ArrayList<String>();
		// tName.cName or cName
		ArrayList<String> original_cNameList = new ArrayList<String>();
		
		// columnNames: <tName.cName ...> 
		// asterisk가 있으면 rename은 신경쓸 필요 없음! 어차피 column name을 rename하지 않음 
		if(!isAsterisk) {
			
			// selectList에서 이미 fromList에서 rename된 tName으로 써야함 
			// <(tName.)cName@cName>
			for(String cName : selectList) {
				if(cName.contains("@")) {
					String[] renamed = cName.split("@");
					original_cNameList.add(renamed[0]);
					renamed_cNameList.add(renamed[1]);
				}else {
					original_cNameList.add(cName);
					renamed_cNameList.add(cName);
				}
			}
			
			for(String cName : original_cNameList) {
				if(cName.contains(".")) {
					
					boolean hasTable = false;
					boolean hasColumn = false;
					for(String c : columnNames) {
						if(c.split("\\.")[0].equals(cName.split("\\.")[0])) {
							hasTable = true;
						}
						if(c.split("\\.")[1].equals(cName.split("\\.")[1])) {
							hasColumn = true;
						}
					}
					if(hasTable == false) {
						throw new myTableException("Selection has failed: '" + cName.split("\\.")[0] + "' does not exist");
					}else if(hasColumn == false) {
						throw new myTableException("Selection has failed: fail to resolve '" + cName.split("\\.")[1] + "'");
					}

					
				}else {
					boolean hasColumn = false;
					for(String c : columnNames) {
						if(c.split("\\.")[1].equals(cName)){
							hasColumn = true;
							break;
						}
					}
					if(hasColumn == false) {
						throw new myTableException("Selection has failed: fail to resolve '" + cName + "'");
					}
				}
				// 여기서 tName.cName으로 columnNames에 있는데, 이 포문에서의 cName은 tName이 없을 수도 있음 그거 구분 
			}
		}

		
		// select문이 없는 경우 (*)
		if(isAsterisk) {
			
			String t = columnNames.get(0).split("\\.")[0];
			boolean oneTable = true;
			for(String c : columnNames) {
				if(!t.equals(c.split("\\.")[0])) {
					oneTable = false;
					break;
				}
			}
			ArrayList<String> only_cNameList = new ArrayList<String>();
			if(oneTable) {
				for(String c : columnNames) {
					String only_cName = c.split("\\.")[1];
					only_cNameList.add(only_cName);
				}
			}else {
				only_cNameList.addAll(columnNames);
			}

			int n = columnNames.size();
			ArrayList<Integer> longestList = new ArrayList<Integer>();
			
			for(int i = 0; i < n; i++) {
				int longest;
				// 우선 칼럼 이름으로 설정 
				longest = only_cNameList.get(i).length();
				// 모든 row 순회하면서 가장 길이가 긴 value로 설정 
				for(int j = 0; j < chosenRows.size(); j++) {
					if(chosenRows.get(j).get(i).length() > longest) {
						longest = chosenRows.get(j).get(i).length();
					}
				}
				longestList.add(longest);
			}
			
			System.out.print("+");
			for(int longest : longestList) {
				for(int i = 0; i < longest + 4; i++)
					System.out.print("-");
				System.out.print("+");
			}
			
			System.out.println();
			System.out.print("|");
			int q = 0;
			for(String cName: only_cNameList) {
				for(int i = 0; i < longestList.get(q) + 2 - cName.length(); i++) {
					System.out.print(" ");
				}
				System.out.print(cName + "  ");
				System.out.print("|");
				q++;
			}
			System.out.println();
			
			System.out.print("+");
			for(int longest : longestList) {
				for(int i = 0; i < longest + 4; i++)
					System.out.print("-");
				System.out.print("+");
			}
			
			System.out.println();


			for(ArrayList<String> chosenRow : chosenRows) {
				System.out.print("|");
				int w = 0;
				for(String val : chosenRow) {
					
					for(int i = 0; i < longestList.get(w) + 2 - val.length(); i++) {
						System.out.print(" ");
					}
					System.out.print(val + "  ");
					System.out.print("|");
					w++;
				}
				System.out.println();
			}
			
			System.out.print("+");
			for(int longest : longestList) {
				for(int i = 0; i < longest + 4; i++)
					System.out.print("-");
				System.out.print("+");
			}
			System.out.println();
		}
		
		// select문이 있는 경우 
		else {
			
			String t = columnNames.get(0).split("\\.")[0];
			boolean oneTable = true;
			for(String c : columnNames) {
				if(!t.equals(c.split("\\.")[0])) {
					oneTable = false;
					break;
				}
			}
			
			// 만약 테이블이 하나라면 테이블 이름을 제거하고 출력 
			if(oneTable) {
				int a = renamed_cNameList.size();
				for(int i = 0; i < a; i++) {
					if(renamed_cNameList.get(i).contains(".")) {
						String only_c = renamed_cNameList.get(i).split("\\.")[1];
						renamed_cNameList.remove(i);
						renamed_cNameList.add(i, only_c);
					}
				}
			}
			
			// chosenRows에서 어떤 칼럼을 프린트 해야하는지 알려주는 인덱스 
			ArrayList<Integer> printIndex = new ArrayList<Integer>();
			
			ArrayList<String> columnNames_onlyC = new ArrayList<String>();
			for(String c : columnNames) {
				String only_c = c.split("\\.")[1];
				columnNames_onlyC.add(only_c);
			}
			
			// columnNames -> tName.cName (모든 조인된 칼럼 이름) 
			// renamed_cNameList -> 바뀐 이름이 있거나, tName.cName 이거나, cName 
			for(String cName : original_cNameList) {
				int pIndex;
				if(cName.contains(".")) {
					pIndex = columnNames.indexOf(cName);
				}else {
					pIndex = columnNames_onlyC.indexOf(cName);
				}
				printIndex.add(pIndex);
			}
			
			int n = printIndex.size();
			
			ArrayList<Integer> longestList = new ArrayList<Integer>();
			
			int k = 0;
			for(int printI : printIndex) {
				int longest;
				// 우선 칼럼 이름으로 최대길이 설정 
				longest = renamed_cNameList.get(k++).length();
				// 모든 row 순회하면서 가장 길이가 긴 value로 설정 
				for(int j = 0; j < chosenRows.size(); j++) {
					if(chosenRows.get(j).get(printI).length() > longest) {
						longest = chosenRows.get(j).get(printI).length();
					}
				}
				longestList.add(longest);
			}
			
			
			System.out.print("+");
			for(int longest : longestList) {
				for(int i = 0; i < longest + 4; i++) {
					System.out.print("-");
				}
				System.out.print("+");
			}
			System.out.println();
			System.out.print("|");
			
			int q = 0;
			for(String cName : renamed_cNameList) {

				for(int i = 0; i < longestList.get(q) + 2 - cName.length(); i++) {
					System.out.print(" ");
				}
				System.out.print(cName + "  ");
				System.out.print("|");
				q++;
			}
			System.out.println();
			
			System.out.print("+");
			for(int longest : longestList) {
				for(int i = 0; i < longest + 4; i++) {
					System.out.print("-");
				}
				System.out.print("+");
			}
			System.out.println();
			
			for(ArrayList<String> chosenRow : chosenRows) {
				System.out.print("|");
				int w = 0;
				for(int printI : printIndex) {
					String val = chosenRow.get(printI);
					for(int i = 0; i < longestList.get(w) + 2 - val.length(); i++) {
						System.out.print(" ");
					}
					System.out.print(val + "  ");
					System.out.print("|");
					w++;
				}
				System.out.println();

			}
			
			System.out.print("+");
			for(int longest : longestList) {
				for(int i = 0; i < longest + 4; i++) {
					System.out.print("-");
				}
				System.out.print("+");
			}
			System.out.println();
			
		}


	}
	
	
	public static void deleteRowDB(Database tableDB, Database tupleDB, String tableName, WhereClause WC) throws myTableException, Exception {
		// 지우고자 하는 테이블과 튜플 정보 
		Table thisTable;
		Tuple thisTuple;
		
		if(DBmanage.getData(tableDB, tableName) == null) {
			throw new myTableException("No such table");
		}else {
			thisTable = (Table) DBmanage.getData(tableDB, tableName);
		}
		
		if(DBmanage.getTupleData(tupleDB, tableName) == null) {
			throw new myTableException("Table '" + tableName + "' has no value");
		}else {
			thisTuple = (Tuple) DBmanage.getTupleData(tupleDB, tableName);
		}
		
		// where clause 가 없다면 전부 삭제 
		if(WC == null) {
			int rowNumber = thisTuple.getAllRows().size();
			boolean isRefered = false;
			
			ArrayList<String> changingTableList = new ArrayList<String>();
			ArrayList<Tuple> changedTupleList = new ArrayList<Tuple>();
			boolean actuallyDelete = true;
			
			// 지우려는 테이블의 모든 칼럼을 순회 
			for(String cName : thisTable.getColumnNames()) {
				
				// 해당 칼럼을 참조하는 다른 테이블들의 리스트 
				ArrayList<String> referingTables = thisTable.getColumns().get(cName).getReferingTable();
				
				// 참조하는 테이블들이 하나 이상이면 
				if(referingTables.size() > 0) {
					isRefered = true;
										
					// 참조하는 테이블들 모두 순회하면서 해당 칼럼이 nullable한지 체크 
					for(String rTable : referingTables) {
						
						Table refTable = (Table) DBmanage.getData(tableDB, rTable);
						String cNameHere = refTable.getHere_fromThatCol().get(cName);
						boolean isNullable = refTable.getColumns().get(cNameHere).getNull().equals("Y");
						
						// 만약 참조하는 테이블의 해당 칼럼이 nullable 하면 그 칼럼의 모둔 value를 null로 바꾸고 changedTupleList에 넣음 
						if(isNullable) {
//							System.out.println("isNullable 안쪽: cNamehere" + cNameHere);
							Tuple refTuple = (Tuple) DBmanage.getTupleData(tupleDB, rTable);
							refTuple.nullifyColumn(cNameHere);
							changedTupleList.add(refTuple);
							changingTableList.add(rTable);
						// nullable하지 않다면 저장되어있던 수정 tuple과 table 정보를 실제로 데이터베이스에 저장하지 않도록 막음 
						}else {
							actuallyDelete = false;
							
							throw new myTableException("[" + rowNumber + " row(s) are not deleted due to referential integrity");
						}
					}
				}
				
				if(actuallyDelete == false) {
					break;
				}
			}
//			System.out.println("바꿀 튜플들: " + changingTableList);
			
			// delete 실행: 모두 nullable하거나 자신을 참조하는 테이블이 없거나 
			if(actuallyDelete == true) {
				if(isRefered == true) {
					for(int i = 0; i < changingTableList.size(); i++) {
						String tName = changingTableList.get(i);
						Tuple revisedTuple = changedTupleList.get(i);
						DBmanage.deleteFromDB(tupleDB, tName);
						DBmanage.insertToDB(tupleDB, tName, revisedTuple);
						
					}
					DBmanage.deleteFromDB(tupleDB, tableName);
					System.out.println("[" + rowNumber + " row(s) are deleted");
			}
				// 만약 자신을 참조하는 테이블이 하나도 없다면 tuple 전부 삭제 
				else {
					DBmanage.deleteFromDB(tupleDB, tableName);
					System.out.println("[" + rowNumber + " row(s) are deleted");
				}
			}
			
		// Where Clause가 존재하면 !
		}else {
			
			ArrayList<String> columnNames = thisTable.getColumnNames();
			ArrayList<ArrayList<String>> chosenRows = WC.getChosenRows();
			
			int rowNumber = chosenRows.size();
			boolean isRefered = false;
			
			ArrayList<String> changingTableList = new ArrayList<String>();
			ArrayList<Tuple> changedTupleList = new ArrayList<Tuple>();
			boolean actuallyDelete = true;
			
			// 지우려는 테이블의 모든 칼럼을 순회 
			for(String cName : columnNames) {
				int indexColumn = columnNames.indexOf(cName);
				
				// 지워지는 칼럼의 모든 값의 리스트 ... 뒤에서 이 리스트와 참조하고 있는 칼럼들 내의 값들과 비교해서 같은 값을 모두 nullify 
				ArrayList<String> deletingValues = new ArrayList<String>();
				for(ArrayList<String> row : chosenRows) {
					deletingValues.add(row.get(indexColumn));
				}
				
				// 해당 칼럼을 참조하는 다른 테이블들의 리스트 
				ArrayList<String> referingTables = thisTable.getColumns().get(cName).getReferingTable();

				if(referingTables.size() > 0) {
					isRefered = true;
										
					// 참조하는 테이블들 모두 순회하면서 해당 칼럼이 nullable한지 체크 
					for(String rTable : referingTables) {
						
						Table refTable = (Table) DBmanage.getData(tableDB, rTable);
						String cNameHere = refTable.getHere_fromThatCol().get(cName);
						boolean isNullable = refTable.getColumns().get(cNameHere).getNull().equals("Y");
						
						// 만약 참조하는 테이블의 해당 칼럼이 nullable 하면 그 칼럼의 value중 지우려는 value와 일치하는 녀석을 null로 바꾸고 changedTupleList에 넣음 
						if(isNullable) {
//							System.out.println("isNullable 안쪽: cNamehere" + cNameHere);
							Tuple refTuple = (Tuple) DBmanage.getTupleData(tupleDB, rTable);
							refTuple.nullifyMatchingColumn(cNameHere, deletingValues);
							changedTupleList.add(refTuple);
							changingTableList.add(rTable);
						}else {
							actuallyDelete = false;
							
							throw new myTableException("[" + rowNumber + " row(s) are not deleted due to referential integrity");
						}
					}
				}
				
				if(actuallyDelete == false) {
					break;
				}
			}
//			System.out.println("바꿀 튜플들: " + changingTableList);
			
			// delete 실행: 모두 nullable하거나 자신을 참조하는 테이블이 없거나 
			if(actuallyDelete == true) {
				if(isRefered == true) {
					for(int i = 0; i < changingTableList.size(); i++) {
						String tName = changingTableList.get(i);
						Tuple revisedTuple = changedTupleList.get(i);
						DBmanage.deleteFromDB(tupleDB, tName);
						DBmanage.insertToDB(tupleDB, tName, revisedTuple);
						
					}
					
					thisTuple.deleteMatchinRows(chosenRows);
					DBmanage.deleteFromDB(tupleDB, tableName);
					DBmanage.insertToDB(tupleDB, tableName, thisTuple);
					System.out.println("[" + rowNumber + " row(s) are deleted");
			}
				// 만약 자신을 참조하는 테이블이 하나도 없다면 tuple 전부 삭제 
				else {
					thisTuple.deleteMatchinRows(chosenRows);
					DBmanage.deleteFromDB(tupleDB, tableName);
					DBmanage.insertToDB(tupleDB, tableName, thisTuple);
					System.out.println("[" + rowNumber + " row(s) are deleted");
				}
			}
		}	
	}
	
	public static ArrayList<ArrayList<String>> cNameListJoin(ArrayList<Table> tables){
		ArrayList<ArrayList<String>> cNameList = new ArrayList<ArrayList<String>>();
		for(Table table : tables) {
			ArrayList<String> columnNameList = table.getColumnNames();
			cNameList.add(columnNameList);
		}
		return cNameList;
	}
	
	// join method (A X B X C ...)
	public static ArrayList<ArrayList<String>> multipleJoin(ArrayList<Tuple> tupleList, ArrayList<ArrayList<String>> originalRows) {
	
		Tuple tuple2 = tupleList.get(1);
		ArrayList<ArrayList<String>> afterJoin = new ArrayList<ArrayList<String>>();
		
		for(ArrayList<String> row1 : originalRows) {
			for(ArrayList<String> row2 : tuple2.getAllRows()) {
				ArrayList<String> copyrow1 = copyArrayList(row1);
				ArrayList<String> copyrow2 = copyArrayList(row2);
				copyrow1.addAll(copyrow2);
				afterJoin.add(copyrow1);
			}
		}
		tupleList.remove(1);
		if(tupleList.size() > 1) {
			return multipleJoin(tupleList, afterJoin);
		}
		return afterJoin;
	}
	
	
	public static ArrayList<String> copyArrayList(ArrayList<String> a){
		ArrayList<String> copied = new ArrayList<String>();
		for(String str : a) {
			copied.add(str);
		}
		return copied;
	}
	

	public static void createTableDB(String newTableName, Database tableDB, Database tupleDB, ArrayList<String> tElementList) throws Exception, myTableException {
		
		StringSplit ss = new StringSplit(tElementList);
		Table newTable = new Table(newTableName, ss);
		ArrayList<String> referingTables = new ArrayList<String>();
		referingTables = newTable.getReferingTable();
		
		if(DBmanage.getAllKeys(tableDB).contains(newTableName)) {
			throw new myTableException("Create table has failed: table with the same name already exists");
		}else if(referingTables.size() > 0){ //referential check
			for(String otherTableName : referingTables) {			
				// db에 참고하고자 하는 해당 table이 존재하는지?
				if(!DBmanage.getAllKeys(tableDB).contains(otherTableName)) {
					throw new myTableException("Create table has failed: foreign key references non existing table");
				}else {// db에 참고하고자 하는 해당 table이 존재하는 것은 확인한 상!
					
					//참조하는 테이블 객체;
					Table otherTable = (Table)DBmanage.getData(tableDB, otherTableName);
					

					
					// 참조하고 있는 모든 테이블들을 각각 순회하면서 칼럼들을 비교하는 반복문;
					for(String refColumn : newTable.getReferencing().get(otherTableName)) {
						
						// 참조하는 테이블에 foreign key칼럼이 존재하는지?
						if(!otherTable.getColumnNames().contains(refColumn)) {
							throw new myTableException("Create table has failed: foreign key references non existing column");
						}
						// 참조하는 테이블의 칼럼이 primary key가 맞는지?
						else if(!otherTable.getPrimaryKey().contains(refColumn)) {
							throw new myTableException("Create table has failed: foreign key references non primary key column");
						}
						
						else if(!otherTable.getColumns().get(refColumn).getType().equals(newTable.getColumns().get(newTable.getThisTableColumn(refColumn)).getType())) {
							throw new myTableException("Create table has failed: foreign key references wrong type");
						}
												
					}

					ArrayList<String> referencedColumnNames = newTable.getReferencing().get(otherTableName);
					otherTable.putReferenced(newTableName, referencedColumnNames);
					DBmanage.deleteFromDB(tableDB, otherTableName);
					DBmanage.insertToDB(tableDB, otherTableName, otherTable);

					
					
				}
			//	Foreign key의 타입과 foreign key가 참조하는 컬럼의 개수나 타입이 서로 다른 경우, ReferenceTypeError에 해당하는 메시지 출력
			
				
				
			}
		}
		
		
		insertToDB(tableDB, newTableName, newTable);
		System.out.println("'" + newTableName + "' table is created");


	}

	public static void changeReferCount(String referingTable, Database tableDB, boolean toDelete) {
		try {
			Table revisedTable = (Table)DBmanage.getData(tableDB, referingTable);
			revisedTable.reviseReferencedByList(toDelete, referingTable);
			DBmanage.deleteFromDB(tableDB, referingTable);
			DBmanage.insertToDB(tableDB, referingTable, revisedTable);
		} catch (Exception e) {
			// TODO Auto-generated catch block 
			e.printStackTrace();
		}
		
	}
	

	public static void descTableDB(String tableName, Database tableDB) throws Exception, myTableException {

		if(DBmanage.getAllKeys(tableDB).size() == 0){
			throw new myTableException("No such table");
		}else {
			Table retrievedTable = (Table)DBmanage.getData(tableDB, tableName);
			ArrayList<String> columnNames = retrievedTable.getColumnNames();
			LinkedHashMap<String, Column> columns = retrievedTable.getColumns();
			System.out.println("--------------------------------------------------");
			System.out.println("table_name [" + tableName + "]");
			System.out.printf("%-20s", "column_name");
			System.out.printf("%-15s", "type");
			System.out.printf("%-15s", "null");
			System.out.printf("%-15s\n", "key");
			
			for(int i = 0; i < columns.size(); i++) {
				String cname = columnNames.get(i);
				String type = columns.get(cname).getType();
				String isNull = columns.get(cname).getNull();
				String fandp = "";
				if(retrievedTable.getPrimary().contains(cname)) {
					fandp += "PRI";
					if(retrievedTable.getFColumns().contains(cname)) {
						fandp += "/FOR";
					}
				}else if(retrievedTable.getFColumns().contains(cname)) {
					fandp += "FOR";
				}
				
				System.out.printf("%-20s", cname);
				System.out.printf("%-15s", type);
				System.out.printf("%-15s", isNull);
				System.out.printf("%-15s\n", fandp);
			}
			System.out.println("--------------------------------------------------");
		}
	}
	
	public static void showDB(Database tableDB) throws myTableException {
		if(DBmanage.getAllKeys(tableDB).size() == 0) {
			throw new myTableException("There is no table");
		}else {
			System.out.println("----------------");
			for(int i = 0; i < DBmanage.getAllKeys(tableDB).size(); i++) {
				System.out.println(DBmanage.getAllKeys(tableDB).get(i));
			}
			System.out.println("----------------");
		}
	}
	
	public static void dropTableDB(String tableName, Database tableDB, Database tupleDB) throws myTableException, Exception {
		if(!DBmanage.getAllKeys(tableDB).contains(tableName)) {
			throw new myTableException("No such table");
		}else {
			Table table1 = (Table)DBmanage.getData(tableDB, tableName);

			if(table1.getReferenced().size() != 0) {
//			if(table1.referedByHowMany() != 0) {
				throw new myTableException("Drop table has failed: '" + tableName + "' is referenced by other table");
			}else {
				Table thisTable = (Table)DBmanage.getData(tableDB, tableName);
				for(String refTables : thisTable.getReferingTable()) {
//					DBmanage.changeReferCount(refTables, tableDB, true);
					DBmanage.changeColumnData(tableDB, tableName, refTables);
					
					
				}
				DBmanage.deleteFromDB(tableDB, tableName);
				if(DBmanage.getTupleData(tupleDB, tableName) != null) {
					DBmanage.deleteFromDB(tupleDB, tableName);
				}
				System.out.println("'" + tableName + "' table is dropped");

				
				
			}
		}
	}
	
	public static void changeColumnData(Database tableDB, String referingTableName, String referedTableName) {
		try {
			Table rTable;
			if(DBmanage.getData(tableDB, referedTableName) != null) {
				rTable = (Table)DBmanage.getData(tableDB, referedTableName);
				rTable.deleteReferenced(referingTableName);
				DBmanage.deleteFromDB(tableDB, referedTableName);
				DBmanage.insertToDB(tableDB, referedTableName, rTable);
			}
		}catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	
	/*  
	 */
	public static void insertToTable(Database tableDB, Database tupleDB, String tableName, ArrayList<ArrayList<String>> insertingRows) throws Exception, myTableException {
		Table theTable = null;
		if(DBmanage.getData(tableDB, tableName) != null) {
			theTable = (Table)DBmanage.getData(tableDB, tableName);
		}else {
			throw new myTableException("No such table");
		}

		ArrayList<String> newRow = new ArrayList<String>(); // 새로 추가될 row
		
		if(insertingRows.size() == 1) {
			if(theTable.getColumnNames().size() != insertingRows.get(0).size()) {
//				System.out.println(theTable.getColumnNames().size());
//				System.out.println(insertingRows.get(0).size());
				// column이 명시되지 않은 경우 값의 수가 해당 attribute 개수와 다른 경우!
				throw new myTableException("Insertion has failed: Types are not matched");
			}
			newRow = insertingRows.get(0);
		}else {
			ArrayList<String> columnInfo = insertingRows.get(1);
//			System.out.println("columnInfo: " + columnInfo);
			ArrayList<Integer> columnIndexList = new ArrayList<Integer>();
			ArrayList<String> insertedValues = insertingRows.get(0);
			
			for(String c : columnInfo) {
				if(!theTable.getColumnNames().contains(c)) {
					// column 이 명시된 경우 존재하지 않는 칼럼에 값을 넣은 경우!
					throw new myTableException("Insertion has failed: '" + c + "' does not exist");
				}
				columnIndexList.add(theTable.getColumnNames().indexOf(c));
			}
			
			// 칼럼 순서에 맞게 없는 곳은 null, 있는 곳은 value 넣음!
			for(int i = 0; i < theTable.getColumnNames().size(); i++) {
				newRow.add("null");
			}
			//<"null", "null">
//			System.out.println("newRow: " + newRow);
//			System.out.println("columnIndexList: " + columnIndexList);
			
			int j = 0;
			for(int i : columnIndexList) {
				newRow.remove(i);
				newRow.add(i, insertedValues.get(j++));
			}
//			System.out.println("newRow: " + newRow);
		}
		
		/*
		 * 1) null 체크! 
		 * 2) type 체크!
		 * 3) foreign key제약 체크! (참조하는 테이블에 해당 값이 존재하는지)
		 * 4) primary key 제약 체크! (삽입하고 나서 duplicate이 발생한다면 에러)
		 * */
		
		ArrayList<String> addingRow = new ArrayList<String>();
		for(int i = 0; i < theTable.getColumnNames().size(); i++) {
			String cName = theTable.getColumnNames().get(i);
			String value = newRow.get(i);
			String newType = value.substring(0, 1);
			String columnType = theTable.getColumns().get(cName).getType();
			
			//1) null 체크! 2) type 체크!
			
			if(value.equals("null")) {
				if(theTable.getColumns().get(cName).getNull().equals("N")) {
					throw new myTableException("Insertion has failed: '" + cName + "' is not nullable");
				}
			}
			
			if(newType.equals("I")) {
				if(!columnType.equals("int")) {
					throw new myTableException("Insertion has failed: Types are not matched");
				}else {
					value = value.substring(1);
				}
			}
//			System.out.println("value: " + value);
			if(newType.equals("D")) {
				if(!columnType.equals("date")) {
					throw new myTableException("Insertion has failed: Types are not matched");	
				}else {
					value = value.substring(1);
				}
			}
			
			if(newType.equals("C")) {
				if(!columnType.substring(0, 2).equals("ch")) {
					throw new myTableException("Insertion has failed: Types are not matched");
				}else { //char(15)
					int size = Integer.parseInt(columnType.substring(5, columnType.length() - 1));
					if(value.length() > size) {
						value = value.substring(1, 1 + size);
					}else {
						value = value.substring(1);
					}
					
				}
			}
			
			// 3) foreign key 제약체크!
			// 하나하나 value가 foreign column에 있는지 없는지. 
			
			if(!value.equals("null")) {
				if(theTable.getFColumns().contains(cName)) {
					String fTable = theTable.findForeignTable(cName);
					String fColumn = theTable.findForeignColumn(cName);
//					System.out.println("thistable colname: " + cName + " fTable: " + fTable + " fColumn: " + fColumn);
					
					if(DBmanage.getTupleData(tupleDB, fTable) != null) {
						Tuple fTuple = (Tuple)DBmanage.getTupleData(tupleDB, fTable);
//						System.out.println("fTuple column: " + fTuple.getColumn(fColumn));
						if(!fTuple.getColumn(fColumn).contains(value)) {
							throw new myTableException("Insertion has failed: Referential integrity violation");
						}	
						
					}else {
						throw new myTableException("Insertion has failed: Referential integrity violation");
					}					
				}
			}
			
			addingRow.add(value);
		}
		
		
		// 3) referential constraints 어기는 경우 한 가지 더 체크! foreign key들 한 줄이 다 있는지 없는지 
		ArrayList<String> referingTables = theTable.getReferingTable();
		LinkedHashMap<String, ArrayList<String>> thatTablefColumns = theTable.getReferencing();
//		System.out.println("1: " + theTable.getReferencing());
		LinkedHashMap<String, ArrayList<String>> thisTablefColumns = theTable.getFHash();
//		System.out.println("2: " + theTable.getFHash());
		
		for(String fTable : referingTables) {
			if(DBmanage.getTupleData(tupleDB, fTable) != null) {
				Tuple fTuple = (Tuple)DBmanage.getTupleData(tupleDB, fTable);
				ArrayList<String> fColumnList = thatTablefColumns.get(fTable); // 참조되는 쪽에서의 column name
				
//				System.out.println("fTable: " + thisTablefColumns);
				ArrayList<String> fThisColumnList = thisTablefColumns.get(fTable); // 참조하는 쪽에서의 column name
				
				ArrayList<String> fAddingRow = new ArrayList<String>(); // 추가되는 row에서 foreign key들만 뺀 부분 
				
				for(String fc : fThisColumnList) {
					fAddingRow.add(addingRow.get(theTable.getColumnNames().indexOf(fc)));
				}
				
//				System.out.println("fAddingRow: " + fAddingRow);
//				
//				for(int i = 0; i < fTuple.getMultipleColumns(fColumnList).size(); i++) {
//					System.out.println("3: " + fTuple.getMultipleColumns(fColumnList).get(i));
//				}

				if(!fTuple.getMultipleColumns(fColumnList).contains(fAddingRow)) {
					throw new myTableException("Insertion has failed: Referential integrity violation");
				}				
			}else {
				throw new myTableException("Insertion has failed: Referential integrity violation");
			}
		}
		
		
		
		// 4) primary key 제약 체크! (삽입하고 나서 duplicate이 발생한다면 에러)
		
		String pValues = ""; // Tuple 객체 내부의 ArrayList<String> primaryValues에 들어갈 원소 (@val@val@val...의 형태)
		ArrayList<String> pKeys = theTable.getPrimaryKey(); // 순서 변하지 않음! tuple 객체 내부의 pValues 순서 변화하지 않음!
		ArrayList<Integer> pKeysIndex = new ArrayList<Integer>(); // 역시 순서 변하지 않음!
		for(String pKey : pKeys) {
			pKeysIndex.add(theTable.getColumnNames().indexOf(pKey));
		}
		
		for(int index : pKeysIndex) {
			pValues += "@" + addingRow.get(index);
		}
		
//		System.out.println("pValues: " + pValues);
	
		if(DBmanage.getTupleData(tupleDB, tableName) != null) {
			Tuple theTuple = (Tuple)DBmanage.getTupleData(tupleDB, tableName);

//			System.out.println(theTuple.getAllRows().get(0));
//			System.out.println("pvalues in tuple: " + theTuple.getPValues());
			if(theTuple.getPValues().contains(pValues)) {
				throw new myTableException("Insertion has failed: Primary key duplication");
			}
			theTuple.insertRows(addingRow, pValues);
			DBmanage.deleteFromDB(tupleDB, tableName);
			DBmanage.insertToDB(tupleDB, tableName, theTuple);
		}else {
//			System.out.println("no tuple data");
			ArrayList<ArrayList<String>> newTuple = new ArrayList<ArrayList<String>>();
			newTuple.add(addingRow);
			Tuple theTuple = new Tuple(tableName, theTable.getColumnNames(), newTuple, pValues);
			DBmanage.insertToDB(tupleDB, tableName, theTuple);
		}
		System.out.println("The row is inserted");
		

	}


	public static void insertToDB(Database DB, String key, Object obj) {
		Cursor cursor = null;
		DatabaseEntry insertKey;
		DatabaseEntry insertData;
		
		try {
			cursor = DB.openCursor(null, null);
			insertKey = new DatabaseEntry(key.getBytes("UTF-8"));
			insertData = new DatabaseEntry(objToString(obj).getBytes("UTF-8"));
			cursor.put(insertKey, insertData);
		}catch(DatabaseException de) {
			
		}catch(UnsupportedEncodingException ue) {
			ue.printStackTrace();
		}finally {
			cursor.close();
		}
	}
	
	public static void deleteFromDB(Database DB, String key) {
		Cursor cursor = null;
		
		try {
			DatabaseEntry searchKey = new DatabaseEntry(key.getBytes("UTF-8"));
			DatabaseEntry searchData = new DatabaseEntry();
			cursor = DB.openCursor(null, null);
			OperationStatus retVal = cursor.getSearchKey(searchKey, searchData,
                    LockMode.DEFAULT);
			if(retVal == OperationStatus.SUCCESS)
				cursor.delete();
		}catch(DatabaseException dbe) {
			System.err.println("Error accessing database." + dbe);
		}catch(UnsupportedEncodingException ee) {
			ee.printStackTrace();
		}finally {
			cursor.close();
		}
	}
	
	
	public static ArrayList<String> getAllKeys(Database DB){
		Cursor cursor = null;
		ArrayList<String> keyList = new ArrayList<String>();
		DatabaseEntry foundKey = new DatabaseEntry();
		DatabaseEntry foundData = new DatabaseEntry();
		
		try {
			cursor = DB.openCursor(null, null);
			
			while(cursor.getNext(foundKey, foundData, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
				String keyString = new String(foundKey.getData(), "UTF-8");
				keyList.add(keyString);
			}
		}catch(DatabaseException dbe) {
			System.err.println("Error accessing database." + dbe);
		}catch(UnsupportedEncodingException ee) {
			ee.printStackTrace();
		}finally {
			cursor.close();
		}
		return keyList;
	}
	
	
	public static Object getTupleData(Database DB, String key) throws Exception {
		Cursor cursor = null;

		try {
			cursor = DB.openCursor(null, null);
			DatabaseEntry getKey = new DatabaseEntry(key.getBytes("UTF-8"));
			DatabaseEntry getData = new DatabaseEntry();
			OperationStatus retVal = cursor.getSearchKey(getKey, getData, LockMode.DEFAULT);
			String foundKey = null;
			String foundData = null;
			
			if(retVal == OperationStatus.NOTFOUND) {
				return null;
			}else {
				foundKey = new String(getKey.getData(), "UTF-8");
				foundData = new String(getData.getData(), "UTF-8");
			}
			
			byte[] serializedMember = Base64.getDecoder().decode(foundData);
		    try (ByteArrayInputStream bais = new ByteArrayInputStream(serializedMember)) {
		        try (ObjectInputStream ois = new ObjectInputStream(bais)) {
		            // 역직렬화된 Member 객체를 읽어온다.
		            Object objectMember = ois.readObject();
		            Tuple retrieveTuple = (Tuple) objectMember;
		            return retrieveTuple;
		        } catch (ClassNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		    } catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}catch(UnsupportedEncodingException ee) {
			ee.printStackTrace();
		}finally {
			cursor.close();
		}
		return null;
		
	}
	
	
	public static Object getData(Database DB, String key) throws Exception {
		Cursor cursor = null;

		try {
			cursor = DB.openCursor(null, null);
			DatabaseEntry getKey = new DatabaseEntry(key.getBytes("UTF-8"));
			DatabaseEntry getData = new DatabaseEntry();
			OperationStatus retVal = cursor.getSearchKey(getKey, getData, LockMode.DEFAULT);
			String foundKey = null;
			String foundData = null;
			
			if(retVal == OperationStatus.NOTFOUND) {
				throw new Exception("No such table");
			}else {
				foundKey = new String(getKey.getData(), "UTF-8");
				foundData = new String(getData.getData(), "UTF-8");
			}
			
			byte[] serializedMember = Base64.getDecoder().decode(foundData);
		    try (ByteArrayInputStream bais = new ByteArrayInputStream(serializedMember)) {
		        try (ObjectInputStream ois = new ObjectInputStream(bais)) {
		            // 역직렬화된 Member 객체를 읽어온다.
		            Object objectMember = ois.readObject();
		            Table retrieveTable = (Table) objectMember;
		            return retrieveTable;
		        } catch (ClassNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		    } catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}catch(UnsupportedEncodingException ee) {
			ee.printStackTrace();
		}finally {
			cursor.close();
		}
		return null;
		
	}
	
	
	public static String objToString(Object obj) {
		byte[] serializedObj = null;

		try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
	        try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
	            oos.writeObject(obj);
	            // serializedMember -> 직렬화된 member 객체 
	            serializedObj = baos.toByteArray();
	        }
	    } catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return Base64.getEncoder().encodeToString(serializedObj);
		
	}
	
	

}
