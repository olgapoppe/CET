package graph;

import java.io.IOException;
import java.util.ArrayList;
import event.*;
import iogenerator.OutputFileGenerator;

public class Node {
	
	public Event event;
	public ArrayList<Node> previous;
	public ArrayList<Node> following;
	public ArrayList<ArrayList<Node>> results;
	
	public Node (Event e) {
		event = e;
		previous = new ArrayList<Node>();
		following = new ArrayList<Node>();
		results = new ArrayList<ArrayList<Node>>();
	}
	
	public boolean isCompatible(Node other) {
		return this.event.isCompatible(other.event);
	}
	
	public boolean equals(Node other) {
		return this.event.equals(other.event);
	}
	
	public String toString() {
		return event.id + ""; // + " has " + previous.size() + " previuos and " + following.size() + " following events."; 
	}
	
	public int printResults(OutputFileGenerator output) {
		int memory4results = 0;
		for(ArrayList<Node> sequence : results) { 				
			try { output.file.append(sequence.toString() + "\n"); } catch (IOException e) { e.printStackTrace(); }
			memory4results +=sequence.size();
		}	
		return memory4results;
	}
}


