package graph;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;

import event.*;
import iogenerator.OutputFileGenerator;

public class Node {
	
	public Event event;
	public ArrayList<Node> previous;
	public ArrayList<Node> following;
	public boolean isLastNode;
	// Mapping of a first node to all CETs that start with the first node and end with this node
	public HashMap<Node,ArrayList<String>> results; 
	
	public Node (Event e) {
		event = e;
		previous = new ArrayList<Node>();
		following = new ArrayList<Node>();
		isLastNode = false;
		results = new HashMap<Node,ArrayList<String>>();
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
	
	public int getEventNumber (String sequence) {
		int number = 1;
		for (int i=0; i<sequence.length(); i++) {
			if (sequence.substring(i,i+1).equals(";")) number++;
		}
		return number;
	}	
	
	public int printResults(OutputFileGenerator output) {
		int memory4results = 0;
		Set<Node> first_nodes = results.keySet();
		for (Node first_node : first_nodes) {
			ArrayList<String> sequences = results.get(first_node);
			for(String sequence : sequences) { 				
				try { output.file.append(sequence + "\n"); } catch (IOException e) { e.printStackTrace(); }
				memory4results += getEventNumber(sequence);
		}}	
		return memory4results;
	}
	
	public String resultsToString () {
		String result = "";
		Set<Node> first_nodes = results.keySet();
		for (Node first_node : first_nodes) {
			ArrayList<String> sequences = results.get(first_node);
			for(String sequence : sequences) { 				
				result += sequence + " "; 				
		}}	
		return result;
	}
	
	public String toString() {
		return event.id + ""; // + " has " + previous.size() + " previuos and " + following.size() + " following events."; 
	}
}


