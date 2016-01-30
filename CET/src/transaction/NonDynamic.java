package transaction;

import graph.*;

public class NonDynamic {
	
	Graph graph;
	
	public NonDynamic (Graph g) {
		graph = g;
	}
	
	public void traverse() {
		
		for (Node first : graph.first_nodes) {
			dfs(first);
		}		
	}
	
	public void dfs (Node node) {       
        
        System.out.print(node.event.id + ",");
        node.visited = true;

        for(Node following : node.following) {
            if(!following.visited) {
                dfs(following);
            }
        }
    }
}
