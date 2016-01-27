package scheduler;

import java.util.ArrayList;
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
	
	public Scheduler (int last, EventQueue eq, ExecutorService exe, CountDownLatch d, long start, AtomicInteger dp, int wl) {	
		
		lastsec = last;
		eventqueue = eq;
		executor = exe;
		done = d;
		startOfSimulation = start;
		drProgress = dp;
		window_length = wl;
	}
	
	/**
	 * As long as not all events are processed, extract events from the event queue and execute them.
	 */	
	public void run() {	
		
		int progress = -1;
		boolean last_iteration = false;
						
		/*** Get the permission to schedule current second ***/
		while (eventqueue.getDriverProgress(progress)) {
	
			ArrayList<Event> batch = new ArrayList<Event>();
										
			/*** Schedule the current second ***/
			Event event = eventqueue.contents.peek();
			while (event != null && event.sec <= progress) { 
					
				Event e = eventqueue.contents.poll();
				//System.out.println(e.toString());
				batch.add(e);
				event = eventqueue.contents.peek();
			}
			// Create a transaction and submit it for execution
			transaction_number = new CountDownLatch(1);
			BaseLineStatic transaction = new BaseLineStatic(batch,startOfSimulation, transaction_number);
			executor.execute(transaction);
									
			/*** If the stream is over, wait for acknowledgment of the previous transactions and terminate ***/				
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
	}	
}