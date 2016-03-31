package optimizer;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;

import event.*;
import graph.*;

public class BnB_topDown extends Partitioner {	
	
	public BnB_topDown (ArrayDeque<Window> w) {
		super(w);
	}
	
	public Partitioning getPartitioning (ArrayList<Event> batch, double memory_limit) {
		
		// Set local variables
		LinkedList<Partitioning> heap = new LinkedList<Partitioning>();
		Partitioning solution = new Partitioning(new ArrayList<Partition>());
		HashMap<String,Integer> pruned = new HashMap<String,Integer>();
				
		double minCPU = Double.MAX_VALUE;		
		int maxHeapSize = 0;
		int considered_count = 0;
		
		/*** Level search ***/		
		// Get number of necessary cuts
		int number_of_necessary_cuts = getMinNumberOfRequiredPartitions_walkDown(batch,memory_limit);
		System.out.println("Min number of necessary cuts: " + number_of_necessary_cuts);
		
		/*** Node search ***/
		// Get number of possible cuts
		int curr_sec = -1;
		int number_of_min_partitions = 0;
		for(Event event : batch) {
			if (curr_sec < event.sec) {
				curr_sec = event.sec;
				number_of_min_partitions++;
		}}	
		int number_of_possible_cuts = number_of_min_partitions - 1;
		System.out.println("Number of possible cuts: " + number_of_possible_cuts);		
		if (number_of_necessary_cuts > number_of_possible_cuts) number_of_necessary_cuts = number_of_possible_cuts;		
		
		// Get all possibilities to cut, cut the graph and store the nodes in the heap 
		ArrayList<ArrayList<Integer>> cuts = getAllCombinationsOfCuts(number_of_possible_cuts,number_of_necessary_cuts);
		System.out.println("There are " + cuts.size() + " possibilities to cut.");
		
		Partitioning max_partitioning = Partitioning.getPartitioningWithMaxPartition(batch);
		
		for (ArrayList<Integer> cut : cuts) {
			System.out.println(cut.toString());
			Partitioning node = max_partitioning.getPartitioning(cut);		
			heap.add(node);		
		}
		
		/*while (!heap.isEmpty()) {
			
			// Get the next node to process, its costs and children 
			Partitioning temp = heap.poll();			
			if (pruned.containsKey(temp.id)) continue;
			double temp_cpu = temp.getCPUcost(windows, 3);
			double temp_mem = temp.getMEMcost(windows, 3);
			
			ArrayList<Partitioning> children = temp.getChildrenBySplitting();
			
			//System.out.println("Considered: " + temp.toString());
			
			considered_count++;
			
			if (temp_mem > memory_limit) {
				
				// Add children to the heap				
				for (Partitioning child : children) {					
					if (!heap.contains(child) && !pruned.containsKey(child.id)) 
						heap.add(child); 
				} 
				// Update max heap size
				if (maxHeapSize < heap.size()) 
					maxHeapSize = heap.size();
			} else {
				// Update solution
				if (temp_cpu < minCPU) {
					solution = temp;
					minCPU = temp_cpu;
				}
				// Prune the children
				for (Partitioning child : children) {
					pruned.put(child.id, 1);
				}
			}			
		}
		System.out.println("Max heap size: " + maxHeapSize + 
				"\nConsidered: " + considered_count);*/	
		
		//System.out.println("Chosen: " + solution.toString()); 
		
		return solution;		
	}
	
	/*** Get minimal number of required partitions walking the search space top down ***/
	public int getMinNumberOfRequiredPartitions_walkDown(ArrayList<Event> batch, double memory_limit) {	
		
		int event_number = batch.size();
		
		// Find the number of minimal partitions
		int s = 1;
		int e = 0;
		int curr_sec = -1;		
		for(Event event : batch) {
			if (curr_sec < event.sec) {
				curr_sec = event.sec;
				e++;
		}}
		
		// Find the minimal number of required partitions
		int m = 0;
		double ideal_memory = 0;
		int level = 0;
		while (s <= e) {	
			m = s + (e-s)/2;
			ideal_memory = getIdealMEMcost(event_number,m,3);						
			System.out.println("k=" + m + " mem=" + ideal_memory);
			
			if (ideal_memory <= memory_limit) {
				level = m;
				e = m - 1;
			} else {
				s = m + 1;
			}
			System.out.println("s=" + s + " e=" + e + "\n");
		}	
		return (level > 0) ? level : event_number;
	}
	
	public double getIdealMEMcost (int event_number, int partition_number, int algorithm) {
		
		double exp;
		double ideal_memory;
		
		if (algorithm == 1) {
			ideal_memory = event_number;
		} else {
		if (algorithm == 2) {
			ideal_memory = Math.pow(3, event_number) * event_number;			
		} else {
			double vertex_number_per_partition = event_number/new Double(partition_number);
			exp = vertex_number_per_partition/new Double(3);			
			ideal_memory = partition_number * Math.pow(3, exp) * vertex_number_per_partition + event_number;
		}}
		return ideal_memory;
	}
	
	/*** Get all combinations of numbers from 1 to max of length n  ***/
	public ArrayList<ArrayList<Integer>> getAllCombinationsOfCuts (int max, int n) {		
		
		// Fill input array with numbers
		int arr[] = new int[max];		
		for (int i=1; i<=max; i++) {
			arr[i-1] = i;
		}	
				
		// A temporary array to store all combination one by one
		int data[] = new int[n];
		
		// Result accumulator
		ArrayList<ArrayList<Integer>> results = new ArrayList<ArrayList<Integer>>();

		// Get all combinations using temporary array 'data[]'
		return getAllCombinationsOfCutsAux(arr, data, 0, arr.length-1, 0, n, results);	
	}
	
	static ArrayList<ArrayList<Integer>> getAllCombinationsOfCutsAux(int arr[], int data[], int start, int end, int index, int r, ArrayList<ArrayList<Integer>> results) {
		
		// Current combination is ready to be printed, print it
		if (index == r) {
			ArrayList<Integer> result = new ArrayList<Integer> ();
			for (int j=0; j<r; j++) {
				//System.out.print(data[j]+" ");
				result.add(data[j]);
			}
			//System.out.println("");
			results.add(result);
			return results;
		}

		// replace index with all possible elements. The condition
		// "end-i+1 >= r-index" makes sure that including one element
		// at index will make a combination with remaining elements
		// at remaining positions
		for (int i=start; i<=end && end-i+1 >= r-index; i++) {
			data[index] = arr[i];
			results = getAllCombinationsOfCutsAux(arr, data, i+1, end, index+1, r, results);
		}
		return results;
	}
}
