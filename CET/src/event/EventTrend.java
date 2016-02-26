package event;

import graph.*;

public class EventTrend {
	
	public Node first_node;
	
	// A sequence is a string of comma separated event ids
	public String sequence;
	
	public EventTrend (Node fn, String seq) {
		first_node = fn;
		sequence = seq;
	}
	
	public int getEventNumber () {
		int number = 1;
		for (int i=0; i<sequence.length(); i++) {
			if (sequence.substring(i,i+1).equals(";")) number++;
		}
		return number;
	}	
}
