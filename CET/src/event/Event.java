package event;

import java.util.ArrayList;
import java.util.HashMap;

public class Event implements Comparable<Event> {
	
	public int sec;
	public int id;
	public int value;
	// Mapping of window identifier to the pointers of this event within this window
	public HashMap<String,ArrayList<Event>> pointers;
	
	public Event (int s, int i, int v) {
		sec = s;
		id = i;
		value = v;
		pointers = new HashMap<String,ArrayList<Event>>();
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
	
	public static Event parseReal (String line) {
		
		String[] values = line.split(",");
		
		int s = Integer.parseInt(values[1]);
        int i = Integer.parseInt(values[3]);
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
	
	public boolean isCompatible(Event other) {
		return this.value == other.value && this.sec < other.sec;
	}
	
	public boolean equals(Event other) {
		return this.id == other.id;
	}
	
	/** Print this event to console */
	public String toString() {
		return "sec: " + sec + " id: " + id + " value: " + value;
	}	
	
	/** Print this event with pointers to console */
	public String toStringWithPointers(String widnow_id) {
		ArrayList<Event> predecessors = pointers.get(widnow_id);
		String s = id + " : ";
		for (Event predecessor : predecessors) {
			s += predecessor.id + ",";
		}
		return s;
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
