package transaction;

import iogenerator.OutputFileGenerator;

import java.util.ArrayDeque;
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
	ArrayDeque<Window> windows;
	Window window; 
	SharedEventTrends shared_trends;
	
	public Partitioned (ArrayList<Event> b, OutputFileGenerator o, CountDownLatch tn, AtomicLong pT, AtomicInteger mMPW, int ml, int sa, ArrayDeque<Window> ws, Window w, SharedEventTrends sp) {
		super(b,o,tn,pT,mMPW);	
		memory_limit = ml;
		search_algorithm = sa;
		windows = ws;
		window = w;
		shared_trends = sp;
	}

	public void run() {
		
		long start =  System.currentTimeMillis();	
		
		/*** Get an optimal CET graph partitioning ***/
		Partitioning root_partitioning = Partitioning.getPartitioningWithMaxPartition(batch);	
		int size_of_the_graph = root_partitioning.partitions.get(0).vertexNumber + root_partitioning.partitions.get(0).edgeNumber; 
		//System.out.println("Root: " + root_partitioning.toString(windows));
		
		Partitioner partitioner;
		if (search_algorithm==1) {
			partitioner = new Exh_maxPartition(windows);
			optimal_partitioning = partitioner.getPartitioning(root_partitioning, memory_limit);
		} else {
			partitioner = new BandB_maxPartition(windows);
			optimal_partitioning = partitioner.getPartitioning(root_partitioning, memory_limit);
		}		
		//System.out.println("Optimal: " + optimal_partitioning.toString(windows));
		
		/*** Compute results per partition ***/
		int cets_within_partitions = 0;
		for (Partition partition : optimal_partitioning.partitions) {	
			
			//if (partition.isShared(windows)) System.out.println("Shared partition: " + partition.toString());
			
			for (Node first_node : partition.first_nodes) { first_node.isFirst = true; }
			Dynamic.computeResults(partition.last_nodes);
			cets_within_partitions += partition.getCETlength();			
		}
		
		/*** Compute results across partition ***/
		int max_cet_across_partitions = 0;
		if (optimal_partitioning.partitions.size() > 1) {
			for (Node first_node : optimal_partitioning.partitions.get(0).first_nodes) {
				
				for (EventTrend event_trend : first_node.results) {				
					int length = computeResults(event_trend, new Stack<EventTrend>(), max_cet_across_partitions);				
					if (max_cet_across_partitions < length) max_cet_across_partitions = length;		
		}}}	
				
		long end =  System.currentTimeMillis();
		long processingDuration = end - start;
		processingTime.set(processingTime.get() + processingDuration);
		
		int memory = size_of_the_graph + cets_within_partitions + max_cet_across_partitions;
		writeOutput2File(memory);
		transaction_number.countDown();
	}
	
	// DFS recomputing intermediate results
	public int computeResults (EventTrend event_trend, Stack<EventTrend> current_cet, int maxSeqLength) {       
			
		current_cet.push(event_trend);
		//System.out.println("pushed " + event_trend.sequence);
	        
		/*** Base case: We hit the end of the graph. Output the current CET. ***/
	    if (event_trend.last_node.following.isEmpty()) {   
	       	String result = "";        	
	       	Iterator<EventTrend> iter = current_cet.iterator();
	       	int eventNumber = 0;
	       	while(iter.hasNext()) {
	       		EventTrend n = iter.next();
	       		result += n.sequence.toString() + ";";
	       		eventNumber += n.getEventNumber();
	       	}
	       	if (maxSeqLength < eventNumber) maxSeqLength = eventNumber;	
	       	//results.add(result);  
	       	//System.out.println("result " + result);
	   } else {
	   /*** Recursive case: Traverse the following nodes. ***/        	
	       	for(Node first_in_next_partition : event_trend.last_node.following) {        		
	       		//System.out.println("following of " + node.event.id + " is " + following.event.id);
	       		
	       		for (EventTrend next_event_trend : first_in_next_partition.results) {	       			
	       			maxSeqLength = computeResults(next_event_trend, current_cet, maxSeqLength);       			
	       		}	       		       		
	       	}        	
	   }
	   EventTrend top = current_cet.pop();
	   //System.out.println("popped " + top.sequence);
	   
	   return maxSeqLength;
   }
	
	public void writeOutput2File(int memory) {
		
		/*int maxSeqLength = 0;
				
		if (output.isAvailable()) {					
				
			for(String sequence : results) {							 				
				try { output.file.append(sequence + "\n"); } catch (IOException e) { e.printStackTrace(); }
				int eventNumber = getEventNumber(sequence);
				if (maxSeqLength < eventNumber) maxSeqLength = eventNumber;			
			}
			output.setAvailable();
		}*/	
		// Output of statistics
		if (maxMemoryPerWindow.get() < memory) maxMemoryPerWindow.getAndAdd(memory);			
	}
}
