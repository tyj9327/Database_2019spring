package myDBMSTools;
import java.io.Serializable;
import java.util.ArrayList;

public class Column implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 201212506L;
	
	private String title;
	private String notNull = "N";
	private String dataType;
	private ArrayList<String> referedTable;
	
	public Column(String title, String notNull, String dataType) {
		this.title = title;
		this.notNull = notNull;
		this.dataType = dataType;
		this.referedTable = new ArrayList<String>();
	}

	public String getName() {
		return this.title;
	}
	
	public String getType() {
		return this.dataType;
	}
	
	public String getNull() {
		return this.notNull;
	}
	
	public void addReferingTable(String tName) {

		this.referedTable.add(tName);
	}
	
	public ArrayList<String> getReferingTable(){
		return this.referedTable;
	}
	
	public void removeReferingTable(String tName) {
		int index = this.referedTable.indexOf(tName);
		this.referedTable.remove(index);
	}
	
}
