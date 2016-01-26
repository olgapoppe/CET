package scheduler;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import event.*;

public class Scheduler implements Runnable {
	
	int lastsec;
	final EventQueue eventqueue;	
	ExecutorService executor;
	CountDownLatch done;
	long startOfSimulation;
	AtomicInteger drProgress;
	
	public Scheduler (int last, EventQueue eq, ExecutorService exe, CountDownLatch d, long start, AtomicInteger dp) {	
		
		lastsec = last;
		eventqueue = eq;
		executor = exe;
		done = d;
		startOfSimulation = start;
		drProgress = dp;
	}
	
	/**
	 * As long as not all events are processed, iterate over all run task queues and pick tasks to execute in round-robin manner.
	 */	
	public void run() {	
		
		int curr_sec = -1;
				
		/*** Get the permission to schedule current second ***/
		while (curr_sec <= lastsec && eventqueue.getDriverProgress(curr_sec)) {
		
			//try {							
				/*** Schedule the current second ***/
				Event event = eventqueue.contents.peek();
				while (event != null && event.sec == curr_sec) { 				
					Event e = eventqueue.contents.poll();
					System.out.println(e.toString());		
					event = eventqueue.contents.peek();
				}
									
				/*** If the stream is over, wait for acknowledgment of the previous transactions and terminate ***/				
				if (curr_sec == lastsec) {	
					//transaction_number.await();						
					done.countDown();					
				} 
				curr_sec++;	
				
			//} catch (final InterruptedException e) { e.printStackTrace(); }
		}		
		System.out.println("Scheduler is done.");
	}	
}