package event;

public class TimeInterval {
	
	public int start;
	public int end;
		
	public TimeInterval (int s, int e) {
		start = s;
		end = e;		
	}
	
	public boolean contains (double n) {
		return start <= n && n <= end;
	}
	
	public String toString() {
		return "[" + new Double(start).intValue() + "," + new Double(end).intValue() + "]";
	}
}
