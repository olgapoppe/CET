package event;

public class Event {
	
	int id;
	public int sec;
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
	
	/** Print this event to console */
	public String toString() {
		return 	"id: " + id + 
				" sec: " + sec + 
				" value: " + value;
	}	
	
	/** Print this event to file */
	public String print2file() {
		return id + "," + sec + "," + value + "\n";
	}
}
