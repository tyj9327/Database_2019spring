package parsingTools;

public class Predicate {
	
	private String predicate;
	private boolean isNullPredicate;
	private String operand1 = "";
	private String operand2 = "";
	private String operator = "";
	private String nullOperand = "";
//	private boolean operand1HasTable = false;
//	private boolean operand2HasTable = false;
//	private boolean nullOpreandHasTable = false;
//	
	
	public Predicate(String predicate) {
		 // $tName#cName@isnotnull 또는 $cName@isnotnull
		if(predicate.substring(0, 1).equals("$")) {
			isNullPredicate = true;
			this.predicate = predicate.substring(1);
			this.nullOperand = this.predicate;
			
		}else {
			isNullPredicate = false;
			this.predicate = predicate;
			String[] devide = this.predicate.split("$");
			this.operand1 = devide[0];
			this.operand2 = devide[2];
			this.operator = devide[1];
		}

	}
	
	public boolean getIsNullPredicate() {
		return this.isNullPredicate;
	}
	
	public String getPredicateString() {
		return this.predicate;
	}
	
	public String getOperand1() {
		return this.operand1;
	}
	
	public String getOperand2() {
		return this.operand2;
	}
	
	public String getOperator() {
		return this.operator;
	}
	
	public String getNullOperand() {
		return this.nullOperand;
	}
	
}
