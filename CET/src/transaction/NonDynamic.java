package transaction;

import java.util.Stack;

import graph.*;

public class NonDynamic {
	
	Graph graph;
	
	public NonDynamic (Graph g) {
		graph = g;
	}
	
	public void traverse() {
		
		for (Node first : graph.first_nodes) {
			Stack<Node> current_sequence = new Stack<Node>();
			dfs(first,current_sequence);
		}		
	}
	
	public void dfs (Node node, Stack<Node> current_sequence) {       
		
		current_sequence.push(node);
		//System.out.println("pushed " + node.event.id);
        
		/*** Base case: We hit the end of the graph. Output the current CET. ***/
        if (node.following.isEmpty()) {        	
        	for (Node n : current_sequence) {
        		System.out.print(n.event.id + ",");
        	}
        	System.out.println("\n");        	        	
        } else {
        /*** Recursive case: Update the current CET and traverse the following nodes. ***/        	
        	for(Node following : node.following) {        		
        		//System.out.println("following of " + node.event.id + " is " + following.event.id);
        		dfs(following,current_sequence);        		
        	}        	
        }
        current_sequence.pop();
        //System.out.println("poped " + top.event.id);
    }
}
