package myDBMSTools;
import java.util.ArrayList;

public class ArrayListIndex {
	public static int getArrayListIndex(ArrayList<String> a, String b) {
		int index = 0;
		for(int i = 0; i < a.size(); i++) {
			if(a.get(i).equals(b)) {
				index = i;
				break;
			}
		}
		
		return index;
	}
	
	public static int getArrayListIndex(ArrayList<Integer> a, Integer b) {
		int index = 0;
		for(int i = 0; i < a.size(); i++) {
			if(a.get(i) == b) {
				index = i;
				break;
			}
		}
		
		return index;
	}
}
