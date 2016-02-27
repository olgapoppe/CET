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
	SharedPartitions shared_partitions;
	
	public Partitioned (ArrayList<Event> b, OutputFileGenerator o, CountDownLatch tn, AtomicLong pT, AtomicInteger mMPW, int ml, int sa, ArrayDeque<Window> ws, Window w, SharedPartitions sp) {
		super(b,o,tn,pT,mMPW);	
		memory_limit = ml;
		search_algorithm = sa;
		windows = ws;
		window = w;
		shared_partitions = sp;
	}

	public void run() {
		
		long start =  System.currentTimeMillis();	
		
		/*** Get an optimal CET graph partitioning ***/
		Partitioning rootPartitioning = Partitioning.getPartitioningWithMaxPartition(batch);	
		int size_of_the_graph = rootPartitioning.partitions.get(0).vertexNumber + rootPartitioning.partitions.get(0).edgeNumber; 
		//System.out.println(rootPartitioning.toString(windows));
		
		Partitioner partitioner;
		if (search_algorithm==1) {
			partitioner = new Exh_maxPartition(windows);
			optimal_partitioning = partitioner.getPartitioning(rootPartitioning, memory_limit);
		} else {
		//if (search_algorithm==2) {
			partitioner = new BandB_maxPartition(windows);
			optimal_partitioning = partitioner.getPartitioning(rootPartitioning, memory_limit);
		/*} else {
			partitioner = new Gre_minPartitions();
			partitioning = partitioner.getPartitioning(rootPartitioning, memory_limit);
		}*/
		}		
		//System.out.println(optimal_partitioning.toString(windows));
		
		int cets_within_partitions = 0;
		int end_of_last_partition_2_read = 0;
		for (Partition partition : optimal_partitioning.partitions) {	
			
			/*** If this partition is to write, compute CETs per partition, copy CETs from last nodes to first nodes, 
			 * store it in the shared data structure and update the time stamp ***/
			if (partition.is2write(windows, window)) {
			
				for (Node last_node : partition.last_nodes) { last_node.isLastNode = true; }
				Dynamic.computeResults(partition.first_nodes);
				partition.copyResultsFromLast2First();		
				cets_within_partitions += partition.getCETlength();
			
				if (partition.isShared(windows)) {
					shared_partitions.contents.put(partition.id,partition);		
					shared_partitions.setProgress(partition.end);
					
					System.out.println("Wrote partition: " + partition.id);
				}
			/*** If this partition is to read, remember the end of the last partition to read ***/
			} else {
				
				System.out.println("Partition 2 read: " + partition.id);
				if (end_of_last_partition_2_read < partition.end) end_of_last_partition_2_read = partition.end;
			}
		}
		
		/*** Wait till partition results to read are computed and construct CETs across partitions ***/
		int max_cet_across_partitions = 0;
		if (shared_partitions.getProgress(end_of_last_partition_2_read)) {
			
			
		
			/*ArrayList<Node> first_nodes = optimal_partitioning.partitions.get(0).first_nodes;
			for (Node first_node : first_nodes) {
				for (EventTrend event_trend : first_node.results.get(first_node)) {
					Stack<EventTrend> current_cet = new Stack<EventTrend>();
					int length = computeResults(event_trend, current_cet, max_cet_across_partitions);
					if (max_cet_across_partitions < length) max_cet_across_partitions = length;
				}			
			}*/
			
			
			
			
		}
		
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
	       		for (EventTrend new_event_trend : first_in_next_partition.results.get(first_in_next_partition)) {
	       			computeResults(new_event_trend, current_cet, maxSeqLength); 
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
