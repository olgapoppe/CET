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
	// A result is a string of comma separated event ids
	public ArrayList<String> results; 
	
	public Node (Event e) {
		event = e;
		previous = new ArrayList<Node>();
		following = new ArrayList<Node>();
		isLastNode = false;
		results = new ArrayList<String>();
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
		for(String sequence : results) { 				
			try { output.file.append(sequence + "\n"); } catch (IOException e) { e.printStackTrace(); }
			memory4results += getEventNumber(sequence);
		}	
		return memory4results;
	}
	
	public int getEventNumber (String sequence) {
		int number = 1;
		for (int i=0; i<sequence.length(); i++) {
			if (sequence.substring(i,i+1).equals(";")) number++;
		}
		return number;
	}
}


