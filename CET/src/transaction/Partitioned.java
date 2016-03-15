package transaction;

import iogenerator.OutputFileGenerator;

import java.io.IOException;
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
	
	Partitioning resulting_partitioning;
	int memory_limit;
	int part_num;
	int search_algorithm;
	ArrayDeque<Window> windows;
	Window window; 
	SharedPartitions shared_trends;
	ArrayList<String> results;
	
	public Partitioned (ArrayList<Event> b, OutputFileGenerator o, CountDownLatch tn, AtomicLong pT, AtomicInteger mMPW, int ml, int pn, int sa, ArrayDeque<Window> ws, Window w, SharedPartitions sp) {
		super(b,o,tn,pT,mMPW);	
		memory_limit = ml;
		part_num = pn;
		search_algorithm = sa;
		windows = ws;
		window = w;
		shared_trends = sp;
		results = new ArrayList<String>();
	}

	public void run() {	
		
		long start =  System.currentTimeMillis();
		
		/*** Get the ideal memory in the middle of the search space 
		 * to decide from where to start the search: from the top or from the bottom ***/
		int curr_sec = -1;
		int number_of_min_partitions = 0;
		for(Event event : batch) {
			if (curr_sec < event.sec) {
				curr_sec = event.sec;
				number_of_min_partitions++;
		}}		
		int event_number = batch.size();
		double ideal_memory_in_the_middle = getIdealMEMcost(event_number, number_of_min_partitions/2);
		boolean top_down = (memory_limit > ideal_memory_in_the_middle);				
		
		Partitioning input_partitioning;
		int bin_number = 0;
		int bin_size = 0;
		Partitioner partitioner;		
		if (search_algorithm==1) {
			/*** Get the input partitioning ***/
			input_partitioning = top_down ? 
						Partitioning.getPartitioningWithMaxPartition(batch) :
						Partitioning.getPartitioningWithMinPartitions(batch);							
			System.out.println("Input partitioning: " + input_partitioning.toString(windows));			
			partitioner = top_down ? 
					new BandB_maxPartition(windows) :
					new BandB_minPartitions(windows);			
		} else {			
			/*** Get the minimal number of required partitions and their size ***/			 
			bin_number = top_down ?
					getMinNumberOfRequiredPartitions_walkDown(event_number,number_of_min_partitions,memory_limit) :
					getMinNumberOfRequiredPartitions_walkUp(event_number,number_of_min_partitions,memory_limit);
			bin_size = (bin_number==0) ? event_number : event_number/bin_number;
			System.out.println("Bin number: " + bin_number +
						"\nBin size: " + bin_size);
			
			/*** Get the input partitioning ***/
			input_partitioning = Partitioning.getPartitioningWithMinPartitions(batch);						
			partitioner = new BalancedPartitions(windows);			
		}		
		resulting_partitioning = partitioner.getPartitioning(input_partitioning, memory_limit, bin_number, bin_size);
		System.out.println("Result: " + resulting_partitioning.toString(windows));
		
		/*if (!optimal_partitioning.partitions.isEmpty()) {*/
			
		/*** Compute results per partition ***//*
		int cets_within_partitions = 0;
		for (Partition partition : optimal_partitioning.partitions) {	
			
			//if (partition.isShared(windows)) System.out.println("Shared partition: " + partition.toString());
			
			for (Node first_node : partition.first_nodes) { first_node.isFirst = true; }
			Dynamic.computeResults(partition.last_nodes);
			cets_within_partitions += partition.getCETlength();			
		}		
		
		*//*** Compute results across partition ***//*
		int max_cet_across_partitions = 0;
		for (Node first_node : optimal_partitioning.partitions.get(0).first_nodes) {
				
			for (EventTrend event_trend : first_node.results) {				
				int length = computeResults(event_trend, new Stack<EventTrend>(), max_cet_across_partitions);				
				if (max_cet_across_partitions < length) max_cet_across_partitions = length;		
		}}*/			
				
		long end =  System.currentTimeMillis();
		long processingDuration = end - start;
		processingTime.set(processingTime.get() + processingDuration);
		
		//int memory = size_of_the_graph + cets_within_partitions + max_cet_across_partitions;
		//writeOutput2File(memory);
		//}
		transaction_number.countDown();		
	}
	
	public double getIdealMEMcost (int n, int k) {
		
		double exp;
		double ideal_memory;
		
		if (k == 0) {			
			exp = n/new Double(3);
			ideal_memory = Math.pow(3, exp) * n;			
		} else {			
			double vertex_number_per_partition = n/new Double(k);
			exp = vertex_number_per_partition/new Double(3);			
			ideal_memory = k * Math.pow(3, exp) * vertex_number_per_partition;			
		}	
		return ideal_memory;
	}
	
	/*** Get minimal number of required partitions walking the search space top down ***/
	public int getMinNumberOfRequiredPartitions_walkDown(int event_number, int number_of_min_partitions, int memory_limit) {	
		
		// Find the minimal number of required partitions (T-CET, H-CET)
		for (int k=0; k<number_of_min_partitions; k++) {	
			double ideal_memory = getIdealMEMcost(event_number,k);
						
			System.out.println("k=" + k + " mem=" + ideal_memory);
			
			if (ideal_memory <= memory_limit) return k;
		}	
		// Each event is in a separate partition (M-CET)
		if (event_number <= memory_limit) return event_number;
		
		// Partitioning does not reduce the memory enough
		return -1;
	}
	
	/*** Get minimal number of required partitions walking the search space bottom up ***/
	public int getMinNumberOfRequiredPartitions_walkUp(int event_number, int number_of_min_partitions, int memory_limit) {	
		
		// Partitioning does not reduce the memory enough
		int result = -1;
		
		// Each event is in a separate partition (M-CET)
		if (event_number <= memory_limit) result = event_number;
		
		// Find the minimal number of required partitions (T-CET, H-CET)
		for (int k=number_of_min_partitions-1; k>=0; k--) {
			double ideal_memory = getIdealMEMcost(event_number,k);
			if (ideal_memory <= memory_limit) {
				result = k;
				
				System.out.println("k=" + k + " mem=" + ideal_memory);
			} else {
				break;
			}
		}				
		return result;
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
	       	String s = (!result.isEmpty()) ? result : event_trend.sequence;
	       	// results.add(s);  
	       	// System.out.println("result " + result);
	   } else {
	   /*** Recursive case: Traverse the following nodes. ***/        	
	       	for(Node first_in_next_partition : event_trend.last_node.following) {        		
	       		// System.out.println("following of " + node.event.id + " is " + following.event.id);
	       		
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
		
		int maxSeqLength = 0;
				
		if (output.isAvailable()) {					
				
			for(String sequence : results) {							 				
				try { output.file.append(sequence + "\n"); } catch (IOException e) { e.printStackTrace(); }
				int eventNumber = getEventNumber(sequence);
				if (maxSeqLength < eventNumber) maxSeqLength = eventNumber;			
			}
			output.setAvailable();
		}	
		// Output of statistics
		if (maxMemoryPerWindow.get() < memory) maxMemoryPerWindow.getAndAdd(memory);			
	}
}
