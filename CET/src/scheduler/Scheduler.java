package scheduler;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import event.*;
import transaction.*;

public class Scheduler implements Runnable {
	
	int lastsec;
	final EventQueue eventqueue;	
	ExecutorService executor;
	CountDownLatch transaction_number;
	CountDownLatch done;
	long startOfSimulation;
	AtomicInteger drProgress;
	int window_length;
	boolean incremental;
	
	public Scheduler (int last, EventQueue eq, ExecutorService exe, CountDownLatch d, long start, AtomicInteger dp, int wl, boolean incr) {	
		
		lastsec = last;
		eventqueue = eq;
		executor = exe;
		done = d;
		startOfSimulation = start;
		drProgress = dp;
		window_length = wl;
		incremental = incr;
	}
	
	/**
	 * As long as not all events are processed, extract events from the event queue and execute them.
	 * Sliding window
	 */	
	public void run() {	
		
		try {		
		/*** Set local variables ***/
		int window_end = Math.min(window_length,lastsec);
		int progress = (incremental) ? 0 : window_end;		
		HashSet<TreeSet<Event>> results = new HashSet<TreeSet<Event>>();
							
		/*** Get the permission to schedule current second ***/
		while (progress <= lastsec && eventqueue.getDriverProgress(progress)) {
			
			ArrayList<Event> batch = new ArrayList<Event>();
			if (incremental) progress = Math.min(eventqueue.driverProgress.get(),window_end);
										
			/*** Schedule the available events ***/
			Event event = eventqueue.contents.peek();
			while (event != null && event.sec <= progress) { 
					
				Event e = eventqueue.contents.poll();
				//System.out.println(e.toString());
				batch.add(e);
				event = eventqueue.contents.peek();
			}
			/*** Create a transaction and submit it for execution ***/
			if (!batch.isEmpty()) {
				transaction_number = new CountDownLatch(1);
				BaseLine transaction = new BaseLine(batch,startOfSimulation,transaction_number,results);
				executor.execute(transaction);
			}
			/*** If the stream is over, wait for acknowledgment of the previous transactions and terminate ***/				
			if (progress == lastsec) {
				transaction_number.await(); 						
				done.countDown();	
				break;
			} else {
				/*** If window is over, wait till results are saved to file, clear the results and update the window end ***/
				if (progress == window_end) {
					transaction_number.await();
					results.clear();
					window_end += window_length;				 
					if (window_end >= lastsec) window_end = lastsec;
				}
				/*** Update progress ***/
				if (incremental) {
					transaction_number.await();
					progress++;
				} else {
					progress = window_end;
				}
			}			
		}
		} catch (InterruptedException e) { e.printStackTrace(); }
		System.out.println("Scheduler is done.");
	}	
	
	/**
	 * As long as not all events are processed, extract events from the event queue and execute them.
	 * Tumbling window
	 */	
	/*public void run() {	
		
		int progress = -1;
		boolean last_iteration = false;
						
		*//*** Get the permission to schedule current second ***//*
		while (eventqueue.getDriverProgress(progress)) {
	
			ArrayList<Event> batch = new ArrayList<Event>();
										
			*//*** Schedule the current second ***//*
			Event event = eventqueue.contents.peek();
			while (event != null && event.sec <= progress) { 
					
				Event e = eventqueue.contents.poll();
				//System.out.println(e.toString());
				batch.add(e);
				event = eventqueue.contents.peek();
			}
			// Create a transaction and submit it for execution
			transaction_number = new CountDownLatch(1);
			BaseLineStatic transaction = new BaseLineStatic(batch,startOfSimulation,transaction_number);
			executor.execute(transaction);
									
			*//*** If the stream is over, wait for acknowledgment of the previous transactions and terminate ***//*				
			if (last_iteration) {
				try { transaction_number.await(); } catch (InterruptedException e) { e.printStackTrace(); }						
				done.countDown();	
				break;
			} else {
				progress += window_length;				 
				if (progress >= lastsec) {
					progress = lastsec;
					last_iteration = true;
				}
			}			
		}		
		System.out.println("Scheduler is done.");
	}	*/
}