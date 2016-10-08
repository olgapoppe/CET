package event;

import java.util.ArrayList;
import java.util.HashMap;

public abstract class Event implements Comparable<Event> {
	
	public int sec;
	public int id;
	// Mapping of window identifier to the pointers of this event within this window     
	public HashMap<String,ArrayList<Event>> pointers;
	
	public Event (int s, int i) {
		sec = s;
		id = i;
		pointers = new HashMap<String,ArrayList<Event>>();
	}	
	
	public static Event parse (String line, String type) {
		Event event;
		if (type.equals("check")) { 
			event = CheckEvent.parse(line); 
		} else {
		if (type.equals("activity")) { 
			event = ActivityEvent.parse(line); 
		} else {
		if (type.equals("stock")) {
			event = StockEvent.parse(line); 
		} else {
			event = null;
			System.err.println("Unexpected event type");
		}}}
		return event;
	}
	
	public int compareTo(Event other) {
		if(this.sec > other.sec){
            return 1;
        } else {
            return -1;
        }
    }
	
	public boolean equals(Event other) {
		return this.id == other.id;
	}
	
	/** Print this event to console */
	public String toString() {
		return "" + id;
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
		return sec + "," + id + "\n";
	}
	
	/** Print this event in a sequence to file */
	public String print2fileInASeq() {
		return sec + "," + id + "; ";
	}
	
	public abstract boolean isCompatible(Event other);
}
