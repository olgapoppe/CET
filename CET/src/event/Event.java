package event;

public class Event implements Comparable<Event> {
	
	public int sec;
	public int id;
	int value;
	
	public Event (int s, int i, int v) {
		sec = s;
		id = i;
		value = v;
	}
	
	public static Event parse (String line) {
		
		String[] values = line.split(",");
		
		int s = Integer.parseInt(values[0]);
        int i = Integer.parseInt(values[1]);
        int v = Integer.parseInt(values[2]);          	
    	    	    	
    	Event event = new Event(s,i,v);    	
    	//System.out.println(event.toString());    	
        return event;
	}
	
	public int compareTo(Event other) {
		if(this.value == other.value && this.sec > other.sec){
            return 1;
        } else {
            return -1;
        }
    }
	
	/** Print this event to console */
	public String toString() {
		return "sec: " + sec + " id: " + id + " value: " + value;
	}	
	
	/** Print this event to file */
	public String print2file() {
		return sec + "," + id + "," + value + "\n";
	}
	
	/** Print this event in a sequence to file */
	public String print2fileInASeq() {
		return sec + "," + id + "," + value + "; ";
	}
}
