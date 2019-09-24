package myDBMSTools;

public class MyDBException extends Exception {
	
	String exceptionMsg = null;
	
	public MyDBException(String message) {
		this.exceptionMsg = message;
		// TODO Auto-generated constructor stub
	}
	
}
