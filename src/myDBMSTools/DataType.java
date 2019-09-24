package myDBMSTools;
import java.io.Serializable;

public class DataType implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 201212506L;
	private String type = null;
	private int charLength = -1;
	
	public DataType(String type) {
		this.type = type;
	}
	
	public void charLength(int length) {
		this.charLength = length;
	}
}
