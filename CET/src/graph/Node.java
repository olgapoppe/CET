package graph;

import java.util.ArrayList;
import event.*;

public class Node {
	
	Event event;
	ArrayList<Node> previous;
	ArrayList<Node> following;
	
	public Node (Event e) {
		event = e;
		previous = new ArrayList<Node>();
		following = new ArrayList<Node>();
	}
	
	public boolean isCompatible(Node other) {
		return this.event.isCompatible(other.event);
	}
	
	public boolean equals(Node other) {
		return this.event.equals(other.event);
	}
}


