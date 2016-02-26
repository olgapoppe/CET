package transaction;

import iogenerator.OutputFileGenerator;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Stack;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import event.*;
import graph.*;
import optimizer.*;

public class Partitioned extends Transaction {
	
	Partitioning optimal_partitioning;
	int memory_limit;
	int search_algorithm;
	
	public Partitioned (ArrayList<Event> b, OutputFileGenerator o, CountDownLatch tn, AtomicLong pT, AtomicInteger mMPW, int ml, int sa) {
		super(b,o,tn,pT,mMPW);	
		memory_limit = ml;
		search_algorithm = sa;
	}

	public void run() {
		
		long start =  System.currentTimeMillis();	
		
		/*** Get an optimal CET graph partitioning ***/
		Partitioning rootPartitioning = Partitioning.getPartitioningWithMaxPartition(batch);		
		System.out.println(rootPartitioning.toString());
		
		Partitioner partitioner;
		if (search_algorithm==1) {
			partitioner = new Exh_maxPartition();
			optimal_partitioning = partitioner.getPartitioning(rootPartitioning, memory_limit);
		} else {
		//if (search_algorithm==2) {
			partitioner = new BandB_maxPartition();
			optimal_partitioning = partitioner.getPartitioning(rootPartitioning, memory_limit);
		/*} else {
			partitioner = new Gre_minPartitions();
			partitioning = partitioner.getPartitioning(rootPartitioning, memory_limit);
		}*/
		}		
		System.out.println(optimal_partitioning.toString());
		
		/*** Compute CETs per partition and copy CETs from last nodes to first nodes ***/
		for (Partition partition : optimal_partitioning.partitions) {			
			for (Node last_node : partition.last_nodes) { last_node.isLastNode = true; }
			Dynamic.computeResults(partition.first_nodes);
			partition.copyResultsFromLast2First();			
		}
		
		/*** Construct complete CETs across partitions ***/
		/*for (Node first : graph.first_nodes) {
			Stack<Node> current_sequence = new Stack<Node>();
			maxSeqLength = computeResults(first,current_sequence,maxSeqLength);
		}*/		
		long end =  System.currentTimeMillis();
		long processingDuration = end - start;
		processingTime.set(processingTime.get() + processingDuration);
		
		//writeOutput2File();
		transaction_number.countDown();
	}
	
	// DFS recomputing intermediate results
	public int computeResults (Node node, Stack<Node> current_sequence, int maxSeqLength) {       
			
		current_sequence.push(node);
		//System.out.println("pushed " + node.event.id);
	        
		/*** Base case: We hit the end of the graph. Output the current CET. ***/
	    if (node.following.isEmpty()) {   
	       	String result = "";        	
	       	Iterator<Node> iter = current_sequence.iterator();
	       	while(iter.hasNext()) {
	       		Node n = iter.next();
	       		result += n.toString() + ";";
	       	}
	       	int eventNumber = getEventNumber(result);
			if (maxSeqLength < eventNumber) maxSeqLength = eventNumber;	
	       	//results.add(result);  
	       	//System.out.println("result " + result);
	   } else {
	   /*** Recursive case: Traverse the following nodes. ***/        	
	       	for(Node following : node.following) {        		
	       		//System.out.println("following of " + node.event.id + " is " + following.event.id);
	       		computeResults(following,current_sequence,maxSeqLength);        		
	       	}        	
	   }
	   Node top = current_sequence.pop();
	   //System.out.println("popped " + top.event.id);
	        
	   return maxSeqLength;
   }
}
