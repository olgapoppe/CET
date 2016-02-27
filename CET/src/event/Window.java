package event;

import graph.Partition;
import java.util.ArrayList;

public class Window {
	
	public int start;
	public int end;
	public ArrayList<Event> events;
	public int event_number;
		
	public Window (int s, int e) {
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
		
	public String toString() {
		return "[" + start + "," + end + "] with " + events.size() + " events.";
	}
}
