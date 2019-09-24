package parsingTools;

import java.util.ArrayList;
import java.util.Arrays;

import com.sleepycat.je.Database;
import myDBMSTools.*;

public class BooleanDefiner {
	
	public static ArrayList<ArrayList<String>> getFinalRows(String[] matchingIndex, ArrayList<ArrayList<String>> joinedRows){
		ArrayList<ArrayList<String>> finalRows = new ArrayList<ArrayList<String>>();
	
		
		for(int i = 0; i < matchingIndex.length; i++) {
			if(matchingIndex[i].equals("T")) {
				finalRows.add(joinedRows.get(i));
			}
		}
		
	
		return finalRows;
	}
	
	public static String[] afterOrMatchingRows(ArrayList<String[]> boolTermList) {
		
		
		int listSize = boolTermList.size();
		String[] edittedArr = new String[boolTermList.get(0).length];
		
		for(int i = 0; i < boolTermList.get(0).length; i++) {
			edittedArr[i] = boolTermList.get(0)[i];
		}
		
		if(listSize > 1) {
			for(int i = 1; i < listSize; i++) {
				for(int j = 0; j < edittedArr.length; j++) {
					String a = edittedArr[j];
					String b = boolTermList.get(i)[j];
					
					// T T, T F, T U, F T, U T
					if(a.equals("T") || b.equals("T")) {
						edittedArr[j] = "T";
					
					// F F 
					}else if(a.equals("F") && b.equals("F")) {
						edittedArr[j] = "F";
						
					// U F, U U, F U	
					}else {
						edittedArr[j] = "U";
					}
					
				}
			}
		}
		
		return edittedArr;
	}
	
	public static String[] afterAndMatchingRows(ArrayList<String[]> boolFactorList) {
		

		
		int listSize = boolFactorList.size();
		String[] edittedArr = new String[boolFactorList.get(0).length];
		
		for(int i = 0; i < boolFactorList.get(0).length; i++) {
			edittedArr[i] = boolFactorList.get(0)[i];
		}
		
		if(listSize > 1) {
			for(int i = 1; i < listSize; i++) {
				for(int j = 0; j < edittedArr.length; j++) {
					String a = edittedArr[j];
					String b = boolFactorList.get(i)[j];
					
						// T T
					if(a.equals("T") && b.equals("T")) {
						edittedArr[j] = "T";
						
						// F F, F T, F U, T F, U F
					}else if(a.equals("F") || b.equals("F")) {
						edittedArr[j] = "F";
						
						// T U, U T, U U
					}else {
						edittedArr[j] = "U";
					}
				}				
			}
		}
		
		return edittedArr;
		

	}
	
	public static String[] afterNotMatchingRows(String[] before, String notInfo) {
		String[] edittedArr = new String[before.length];
		boolean isNot;
		
		if(notInfo.equals("not")) {
			isNot = true;
		}else {
			isNot = false;
		}
		
		if(isNot) {
			for(int i = 0; i < before.length; i++) {
				if(before[i].equals("T")){
					edittedArr[i] = "F";
				}else if(before[i].equals("F")) {
					edittedArr[i] = "T";
				}else if(before[i].equals("U")) {
					edittedArr[i] = "U";
				}
				
			}
		}else {
			for(int i = 0; i < before.length; i++) {
				edittedArr[i] = before[i];
			}
		}
		
		return edittedArr;
	}
	

	// joinedRows에서 predicate에 알맞는 row의 리스트를 반환 
	public static String[] predicateDefiner(ArrayList<String> dataTypeList, ArrayList<String> joinedColumnNames, ArrayList<ArrayList<String>> joinedRows, String predicateLine) {
		boolean isNullPredicate = isNullPredicate(predicateLine);
		
		String predicate = truncatePredicate(predicateLine);
		String[] matchingRowsIndex = null;
		
		try {
		
		
		// nullPredicate인 경우 // tName#cName@isnotnull 또는 cName@isnull
		

		if(isNullPredicate) {
			
			matchingRowsIndex = nullPredicatesDefiner(predicate, joinedColumnNames, joinedRows);
		
			// null predicate이 아니라 comparative인 경우!!!
		}else {
			
			String operand1, operand2, operator;
			String[] split3 = predicate.split("&");
			operand1 = split3[0];
			operand2 = split3[2];
			operator = split3[1];
			
			matchingRowsIndex = comparativePredicatesDefiner(operand1, operand2, operator, dataTypeList, joinedColumnNames, joinedRows);
			
		}
		}catch(myTableException e) {
			e.printStackTrace();
		}
		
		return matchingRowsIndex;

	}
	
	
	////  predicate의 경우  compOperand&<=&compOperand 형태로 나옴! &로 구
	//	predicate => operand + operator(>, <, >=, <=, =, !=) + operand 
	//              comparable value이거나, (table.)columnName이 operand
				//// TtName.@cName 이거나, comparableValue만 나오거나(I, C, D). 구분은 맨 앞 T 혹은 @로. 
	public static String[] comparativePredicatesDefiner(String operand1, String operand2, String operator,ArrayList<String> dataTypeList, ArrayList<String> joinedColumnNames, ArrayList<ArrayList<String>> joinedRows) throws myTableException{
//		ArrayList<ArrayList<String>> matchingRows;
		String[] matchingRowsIndex;
		
		ArrayList<String> valueList1 = comparativeValueFinder(operand1, joinedColumnNames, joinedRows, dataTypeList);
		ArrayList<String> valueList2 = comparativeValueFinder(operand2, joinedColumnNames, joinedRows, dataTypeList);
		
		String dataType1 = valueList1.get(1);
		String dataType2 = valueList2.get(1);
		
		// char의 경우 길이와 상관없이 비교 가능 
		
		// 두 operand들의 타입이 다른경우 예외처리 
		if(!dataType1.equals(dataType2)) {
			throw new myTableException("Where clause try to compare incomparable values");
		}
		
		matchingRowsIndex = selectRows(dataType1, valueList1, valueList2, operator, joinedRows);			
								
		return matchingRowsIndex;
	}
	
	
	public static String[] selectRows(String dataType, ArrayList<String> value1, ArrayList<String> value2, String operator, ArrayList<ArrayList<String>> joinedRows){

		String[] matchingRowsIndex = new String[joinedRows.size()];
		Arrays.fill(matchingRowsIndex, "F");
		
		int cases = 0;
		// column & column 
		if(value1.get(0).equals("#") && value2.get(0).equals("#")) {
			cases = 1;
		// value & column 
		}else if(value1.get(0).equals("$") && value2.get(0).equals("#")) {
			cases = 2;
		// column & value 
		}else if(value1.get(0).equals("#") && value2.get(0).equals("$")) {
			cases = 3;
		// value & value
		}else if(value1.get(0).equals("$") && value2.get(0).equals("$")) {
			cases = 4;
		}
		
	
		// make value1 and value2 contain only values 
		value1.remove(0);
		value2.remove(0);
		value1.remove(0);
		value2.remove(0);
		
		// column & column 
		if(cases == 1) {
			for(int i = 0; i < joinedRows.size(); i++) {
				if(isSelectedRow(dataType, value1.get(i), value2.get(i), operator, cases).equals("T")) {
					matchingRowsIndex[i] = "T";
				}else if(isSelectedRow(dataType, value1.get(i), value2.get(i), operator, cases).equals("U")) {
					matchingRowsIndex[i] = "U";
				}
			}
		// value & column 
		}else if(cases == 2) {
			for(int i = 0; i < joinedRows.size(); i++) {
				if(isSelectedRow(dataType, value1.get(0).substring(1), value2.get(i), operator, cases).equals("T")) {
					matchingRowsIndex[i] = "T";
				}else if(isSelectedRow(dataType, value1.get(0).substring(1), value2.get(i), operator, cases).equals("U")) {
					matchingRowsIndex[i] = "U";
				}
			}
		// column & value
		}else if(cases == 3) {
			for(int i = 0; i < joinedRows.size(); i++) {
				if(isSelectedRow(dataType, value1.get(i), value2.get(0).substring(1), operator, cases).equals("T")) {
					matchingRowsIndex[i] = "T";
				}else if(isSelectedRow(dataType, value1.get(i), value2.get(0).substring(1), operator, cases).equals("U")){
					matchingRowsIndex[i] = "U";
				}
			}
		// value & value
		}else {
			for(int i = 0; i < joinedRows.size(); i++) {
				if(isSelectedRow(dataType, value1.get(0).substring(1), value2.get(0).substring(1), operator, cases).equals("T")) {
					matchingRowsIndex[i] = "T";
				}else if(isSelectedRow(dataType, value1.get(0).substring(1), value2.get(0).substring(1), operator, cases).equals("U")) {
					matchingRowsIndex[i] = "U";
				}
			}
		}
		

		
		return matchingRowsIndex;
	}
	
	

	// U: unknown, F: false, T: true 
	public static String isSelectedRow(String dataType, String value1, String value2, String operator, int cases) {
		String s = "F";
	
		// null value가 없는 경우 

		if(!value1.equals("null") && !value2.equals("null")) {
		
			if(dataType.equals("int")) {
				
				

				int v1 = Integer.parseInt(value1);
				int v2 = Integer.parseInt(value2);
				
				if(operator.equals(">")) {
					if(v1 > v2) {
						s = "T";
					}
				}else if(operator.equals("<")) {
					if(v1 < v2) {
						s = "T";
					}
				}else if(operator.equals(">=")) {
					if(v1 >= v2) {
						s = "T";
					}
				}else if(operator.equals("<=")) {
					if(v1 <= v2) {
						s = "T";
					}
				}else if(operator.equals("=")) {
					if(v1 == v2) {
						s = "T";
					}
				}else if(operator.equals("!=")) {
					if(v1 != v2) {
						s = "T";
					}
				}
			}else if(dataType.equals("date")) {
				String[] split1 = value1.split("-");
				String[] split2 = value2.split("-");
				String s1 = split1[0] + split1[1] + split1[2];
				String s2 = split2[0] + split2[1] + split2[2];
				int v1 = Integer.parseInt(s1);
				int v2 = Integer.parseInt(s2);
				
				if(operator.equals(">")) {
					if(v1 > v2) {
						s = "T";
					}
				}else if(operator.equals("<")) {
					if(v1 < v2) {
						s = "T";
					}
				}else if(operator.equals(">=")) {
					if(v1 >= v2) {
						s = "T";
					}
				}else if(operator.equals("<=")) {
					if(v1 <= v2) {
						s = "T";
					}
				}else if(operator.equals("=")) {
					if(v1 == v2) {
						s = "T";
					}
				}else if(operator.equals("!=")) {
					if(v1 != v2) {
						s = "T";
					}
				}
			}else {
				String v1 = value1;
				String v2 = value2;
				if(operator.equals(">")) {
					if(v1.compareTo(v2) > 0) {
						s = "T";
					}
				}else if(operator.equals("<")) {
					if(v1.compareTo(v2) < 0) {
						s = "T";
					}
				}else if(operator.equals(">=")) {
					if(v1.compareTo(v2) >= 0) {
						s = "T";
					}
				}else if(operator.equals("<=")) {
					if(v1.compareTo(v2) <= 0) {
						s = "T";
					}
				}else if(operator.equals("=")) {
					if(v1.equals(v2)) {
						s = "T";
					}
				}else if(operator.equals("!=")) {
					if(!v1.equals(v2)) {
						s = "T";
					}
				}
			}				
		}else {
			s = "U";
		}
		
		return s;
	}
	

	public static ArrayList<String> comparativeValueFinder(String operand1, ArrayList<String> joinedColumnNames, ArrayList<ArrayList<String>> joinedRows, ArrayList<String> dataTypeList) throws myTableException{
		
		ArrayList<String> valueList = new ArrayList<String>();
		
		// tableName이 있는 경우 
		if(operand1.contains(".")) {
			String[] split1 = operand1.split("\\.");
			String tName = split1[0];
			String cName = split1[1].substring(1);
			
			int tableIndex = -1;
			for(String columnName : joinedColumnNames) {
				if(columnName.split("\\.")[0].equals(tName)) {
					tableIndex++;
				}
			}
			if(tableIndex == -1) {
				throw new myTableException("Where clause try to reference tables which are not specified");
			}
			

			int columnIndex = -1;
			for(String columnName : joinedColumnNames) {
				
				if(columnName.equals(tName + "." + cName)) {
					columnIndex = joinedColumnNames.indexOf(tName + "." + cName);
				}
			}
			
			if(columnIndex == -1) {
				
				throw new myTableException("Where clause try to reference non existing column");
				
			}
			
			valueList.add("#");
			valueList.add(dataTypeList.get(columnIndex));
			for(ArrayList<String> joinedRow : joinedRows) {
				valueList.add(joinedRow.get(columnIndex));
			}
			
		}else if(operand1.contains("@")){
			String cName = operand1.substring(1);
			
			int columnIndex = 0;
			int index = 0;
			int count = 0;
			
			for(String columnName : joinedColumnNames) {
				String only_cName;
				if(columnName.contains(".")) {
					only_cName = columnName.split("\\.")[1];
				}else {
					only_cName = columnName;
				}
				
			
				if(only_cName.equals(cName)) {
					count++;
					columnIndex = index;
				}
				index++;
			}
			
			
			
			if(count > 1) {
				throw new myTableException("Where clause contains ambiguous reference");
			}else if(count == 0) {
				throw new myTableException("Where clause try to reference non existing column");				
			}else {
				valueList.add("#");
				valueList.add(dataTypeList.get(columnIndex));
				for(ArrayList<String> joinedRow : joinedRows) {
					valueList.add(joinedRow.get(columnIndex));
				}	
			}
		// operand is comparableValues I, C, D
		}else {
			valueList.add("$");
			if(operand1.substring(0, 1).equals("I")) {
				valueList.add("int");
			}else if(operand1.substring(0, 1).equals("C")) {
				valueList.add("char");
			}else if(operand1.substring(0, 1).equals("D")) {
				valueList.add("date");
			}
			valueList.add(operand1);
		}
		
		return valueList;
	}
	
	
	
	// rename 된 테이블 이름으로 찾아야함 
	public static String[] nullPredicatesDefiner(String predicate, ArrayList<String> joinedColumnNames, ArrayList<ArrayList<String>> joinedRows) throws myTableException{
		ArrayList<ArrayList<String>> matchingRows = new ArrayList<ArrayList<String>>();
		String[] matchingRowsIndex = new String[joinedRows.size()];
		Arrays.fill(matchingRowsIndex, "F");
		
		String[] splitFirst = predicate.split("@");
		String columnInfo = splitFirst[0];
		String nullInfo = splitFirst[1];

		// joinedColumnNames에서 미리 만약 테이블이름이 잇엇다면 table.columnName 형식으로 저장!! 
		
		int columnIndex = 0;
		
		if(columnInfo.contains("#")) {
			String[] splitSecond = columnInfo.split("#");
			String tableName = splitSecond[0];
			String columnName = splitSecond[1];
			if(joinedColumnNames.contains(tableName + "." + columnName)) {
				columnIndex = joinedColumnNames.indexOf(tableName + "." + columnName);					
			}else {
				throw new myTableException("Where clause try to reference non existing column");
			}

		}else {
			int index = 0;
			int count = 0;
			for(String cName : joinedColumnNames) {
				
				String only_cName;
				
				if(cName.contains(".")) {
					only_cName = cName.split("\\.")[1];
				}else {
					only_cName = cName;
				}
				
				if(only_cName.equals(columnInfo)) {
					count++;
					columnIndex = index;
				}
				index++;
			}
			if(count > 1) {
				throw new myTableException("Where clause contains ambiguous reference");
			}
			
			if(count == 0) {
				throw new myTableException("Where clause try to reference non existing column");
			}
		}
		
		int index = 0;
		
		for(ArrayList<String> joinedRow : joinedRows) {
			
			if(nullInfo.equals("isnull")) {
				if(joinedRow.get(columnIndex).equals("null")) {
					matchingRows.add(joinedRow);
					matchingRowsIndex[index] = "T";
				}
			}else {
				if(!joinedRow.get(columnIndex).equals("null")) {
					matchingRows.add(joinedRow);
					matchingRowsIndex[index] = "T";
				}
			}
			index++;
		}
		
		return matchingRowsIndex;
	}
	
	// tName#cName@isnotnull 또는 cName@isnotnull
	public static boolean isNullPredicate(String predicateLine) {

		if(predicateLine.substring(0, 1).equals("$")) {
			return true;
			
		}else {
			return false;
		}
	}
	
	public static String truncatePredicate(String predicateLine) {
		if(isNullPredicate(predicateLine)) {
			return predicateLine.substring(1);
		}else {
			return predicateLine;
		}
	}
}
