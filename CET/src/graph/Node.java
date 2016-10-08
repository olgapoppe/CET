package graph;

import java.io.IOException;
import java.util.ArrayList;
import event.*;
import iogenerator.OutputFileGenerator;

public class Node {
	
	public Event event;
	public ArrayList<Node> previous;
	public ArrayList<Node> following;
	public ArrayList<EventTrend> results; 
	public boolean isFirst;
	
	public Node (Event e) {
		event = e;
		previous = new ArrayList<Node>();
		following = new ArrayList<Node>();
		results = new ArrayList<EventTrend>();
		isFirst = false;
	}
	
	public boolean isCompatible(Node other) {
		return this.event.isCompatible(other.event);
	}
	
	public boolean equals(Node other) {
		return this.event.equals(other.event);
	}
	
	public void connect (Node other) {
		if (!this.following.contains(other)) this.following.add(other);
		if (!other.previous.contains(this)) other.previous.add(this);
	}	
	
	public int printResults(OutputFileGenerator output) {
		int memory4results = 0;
		for(EventTrend trend : results) { 				
			try { output.file.append(trend.sequence + "\n"); } catch (IOException e) { e.printStackTrace(); }
			memory4results += trend.getEventNumber();		
		}	
		return memory4results;
	}
	
	public int getMaxLength () {
		int max = 0;
		for(EventTrend trend : results) { 
			int length = trend.getEventNumber();
			if (max < length) max = length;		
		}	
		return max;
	}
	
	public String resultsToString () {
		String result = "";
		for(EventTrend trend : results) { 				
			result += trend.sequence + "\n"; 				
		}	
		return result;
	}
	
	public String toString() {
		return event.id + ""; // + " has " + previous.size() + " previous and " + following.size() + " following events."; 
	}
}


