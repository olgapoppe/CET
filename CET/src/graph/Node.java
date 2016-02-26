package graph;

import java.io.IOException;
import java.util.ArrayList;
import event.*;
import iogenerator.OutputFileGenerator;

public class Node {
	
	public Event event;
	public ArrayList<Node> previous;
	public ArrayList<Node> following;
	public boolean isLastNode;
	public ArrayList<EventTrend> results; 
	
	public Node (Event e) {
		event = e;
		previous = new ArrayList<Node>();
		following = new ArrayList<Node>();
		isLastNode = false;
		results = new ArrayList<EventTrend>();
	}
	
	public boolean isCompatible(Node other) {
		return this.event.isCompatible(other.event);
	}
	
	public boolean equals(Node other) {
		return this.event.equals(other.event);
	}
	
	public void connect (Node other) {
		this.following.add(other);
		other.previous.add(this);
		this.isLastNode = false;
		other.isLastNode = true;
	}	
	
	public int printResults(OutputFileGenerator output) {
		int memory4results = 0;
		for(EventTrend et : results) { 				
			try { output.file.append(et.sequence + "\n"); } catch (IOException e) { e.printStackTrace(); }
			memory4results += et.getEventNumber();
		}	
		return memory4results;
	}
	
	public String toString() {
		return event.id + ""; // + " has " + previous.size() + " previuos and " + following.size() + " following events."; 
	}
}


