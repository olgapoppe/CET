package event;

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
	
	public boolean relevant (Event e) {
		return start <= e.sec && e.sec <= end;
	}
	
	public String toString() {
		return "[" + start + "," + end + "] with " + events.size() + " events.";
	}
}
