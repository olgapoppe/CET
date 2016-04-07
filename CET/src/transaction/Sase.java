package transaction;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Stack;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import event.*;
import iogenerator.*;

public class Sase extends Transaction {
	
	HashSet<String> results;
	Window window;
	
	public Sase (ArrayList<Event> b, OutputFileGenerator o, CountDownLatch tn, AtomicLong time, AtomicInteger mem, Window w) {		
		super(b,o,tn,time,mem);
		results = new HashSet<String>();
		window = w;
	}
	
	public void run () {
		
		long start =  System.currentTimeMillis();
		computeResults();
		long end =  System.currentTimeMillis();
		long duration = end - start;
		total_cpu.set(total_cpu.get() + duration);
		
		System.out.println("Window " + window.id + " has " + results.size() + " results.");		
		//writeOutput2File();		
		transaction_number.countDown();
	}
	
	public void computeResults() {
		
		// Initiate data structures: stack, last events
		Stack<Event> stack = new Stack<Event>();
		ArrayList<Event> lastEvents = new ArrayList<Event>();
		ArrayList<Event> newLastEvents = new ArrayList<Event>();
		int curr_sec = -1;
		int pointerCount = 0;
		
		for (Event event: batch) {
			
			if (!event.pointers.containsKey(window.id)) {
				ArrayList<Event> new_pointers = new ArrayList<Event>();
				event.pointers.put(window.id, new_pointers);
			}
			ArrayList<Event> pointers = event.pointers.get(window.id);
									
			// Store pointers to its predecessors
			if (event.sec == curr_sec) {
				for (Event last : lastEvents) {
					
					if (pointers == null) System.out.println("Pointers null");
					
					if (!pointers.contains(last)) {
						pointers.add(last);
						pointerCount++;
				}}
				newLastEvents.add(event);
			} else {
				lastEvents.clear();
				lastEvents.addAll(newLastEvents);
				for (Event last : lastEvents) {
					if (!pointers.contains(last)) {
						pointers.add(last);
						pointerCount++;
				}}
				newLastEvents.clear();
				newLastEvents.add(event);
				curr_sec = event.sec;
			}			
			// Store the event in a stack
			stack.add(event);
			//System.out.println(window_id + " " + event.toStringWithPointers(window_id));
		}		
		// For each new last event, traverse the pointers to extract CETs
		int maxSeqLength = 0;
		for (Event lastEvent : newLastEvents) {
			maxSeqLength = traversePointers(lastEvent, new Stack<Event>(), maxSeqLength);
		}
		int memory = stack.size() + pointerCount + maxSeqLength;
		total_mem.set(total_mem.get() + memory);
		//if (total_mem.get() < memory) total_mem.getAndAdd(memory);
	}
	
	// DFS in the stack
	public int traversePointers (Event event, Stack<Event> current_sequence, int maxSeqLength) {       
			
		current_sequence.push(event);
		//System.out.println("pushed " + node.event.id);
		
		ArrayList<Event> pointers = event.pointers.get(window.id);
	        
		/*** Base case: We hit the end of the graph. Output the current CET. ***/
	    if (pointers.isEmpty()) {   
	       	String result = "";        	
	       	Iterator<Event> iter = current_sequence.iterator();
	       	while(iter.hasNext()) {
	       		Event n = iter.next();
	       		result += n.id + ";";
	       	}
	       	int eventNumber = getEventNumber(result);
			if (maxSeqLength < eventNumber) maxSeqLength = eventNumber;	
	       	results.add(result);  
	        
			//System.out.println("result " + result);
				
	    } else {
	    /*** Recursive case: Traverse the following nodes. ***/     	
	       	for(Event previous : pointers) {        		
	       		//System.out.println("following of " + node.event.id + " is " + following.event.id);
	       		maxSeqLength = traversePointers(previous,current_sequence,maxSeqLength);        		
	       	}        	
	    }
	    Event top = current_sequence.pop();
	    //System.out.println("popped " + top.event.id);
	    	    
	    return maxSeqLength;
	}
}
