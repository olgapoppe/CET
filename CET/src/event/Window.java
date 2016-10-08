package event;

import graph.Partition;
import java.util.ArrayList;

public class Window {
	
	public String id;
	public int start;
	public int end;
	public ArrayList<Event> events;
	public int event_number;
		
	public Window (int s, int e) {
		id = s + "-" + e;
		start = s;
		end = e;
		events = new ArrayList<Event>();
		event_number = 0;
	}
	
	public boolean equals (Object other) {
		Window w = (Window) other;
		return this.start == w.start && this.end == w.end;
 	}
	
	public boolean relevant (Event e) {
		return start <= e.sec && e.sec <= end;
	}
	
	public boolean expired (Event e) {
		return end < e.sec;
	}
	
	public boolean contains (Partition p) {
		return start <= p.start && p.end <= end;
	}
	
	public boolean contains (String pid) {
		
		String[] array = pid.split(" ");
		int s = Integer.parseInt(array[0]);
		int e = Integer.parseInt(array[1]);
		return start <= s && e <= end;
	}
	
	// The window that starts with this partition writes it
	public boolean writes (int s, boolean overlap) {		
		return this.start == s || !overlap;
	}
		
	public String toString() {
		return "[" + start + "," + end + "] with " + events.size() + " events.";
	}
}
