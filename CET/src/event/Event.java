package event;

public class Event {
	
	int id;
	int sec;
	int value;
	
	public Event (int i, int t, int v) {
		id = i;
		sec = t;
		value = v;
	}
	
	public static Event parse (String line) {
		
		String[] values = line.split(",");
		
		int i = Integer.parseInt(values[0]);
        int t = Integer.parseInt(values[1]);
        int v = Integer.parseInt(values[2]);          	
    	    	    	
    	Event event = new Event(i,t,v);    	
    	//System.out.println(event.toString());    	
        return event;
	}
	
	/** 
	 * Print this activity report.
	 */
	public String toString() {
		return 	"id: " + id + 
				" sec: " + sec + 
				" value: " + value;
	}	
}
