package graph;

import java.util.ArrayList;
import event.*;

public class Node {
	
	public Event event;
	public ArrayList<Node> previous;
	public ArrayList<Node> following;
	public boolean visited;
	
	public Node (Event e) {
		event = e;
		previous = new ArrayList<Node>();
		following = new ArrayList<Node>();
		visited = false;
	}
	
	public boolean isCompatible(Node other) {
		return this.event.isCompatible(other.event);
	}
	
	public boolean equals(Node other) {
		return this.event.equals(other.event);
	}
	
	public String toString() {
		return event.id + " has " + previous.size() + " previuos and " + following.size() + " following events."; 
	}
}


