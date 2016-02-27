package scheduler;

import java.util.ArrayDeque;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import event.*;
import iogenerator.*;
import transaction.*;

public class Scheduler implements Runnable {
	
	final EventQueue eventqueue;
	int lastsec;
	int window_length;
	int window_slide;
	ArrayDeque<Window> windows;
	int algorithm;
	int memory_limit;
	int search_algorithm;
		
	ExecutorService executor;
	
	AtomicInteger drProgress;
	CountDownLatch transaction_number;
	CountDownLatch done;
	
	AtomicLong processingTime;	
	AtomicInteger maxMemoryPerWindow;
	OutputFileGenerator output;
	
	SharedPartitions shared_partitions;
	
	public Scheduler (EventQueue eq, int last, int wl, int ws, int a, int ml, int sa, 
			ExecutorService exe, AtomicInteger dp, CountDownLatch d, AtomicLong pT, AtomicInteger mMPW, OutputFileGenerator o) {	
		
		eventqueue = eq;
		lastsec = last;
		window_length = wl;
		window_slide = ws;
		windows = new ArrayDeque<Window>();
		algorithm = a;
		memory_limit = ml;
		search_algorithm = sa;
		
		executor = exe;
		
		drProgress = dp;
		int window_number = last/ws + 1;
		transaction_number = new CountDownLatch(window_number);
		done = d;
		
		maxMemoryPerWindow = mMPW;
		processingTime = pT;	
		output = o;
		
		shared_partitions = new SharedPartitions(new AtomicInteger(0));
	}
	
	/**
	 * As long as not all events are processed, extract events from the event queue and execute them.
	 */	
	public void run() {	
		
		/*** Create windows ***/	
		ArrayDeque<Window> windows2iterate = new ArrayDeque<Window>();
		int start = 0;
		int end = window_length;
		while (start <= lastsec) {
			Window window = new Window(start, end);		
			windows.add(window);
			windows2iterate.add(window);
			start += window_slide;
			end = (start+window_length > lastsec) ? lastsec : (start+window_length); 
			System.out.println(window.toString() + " is created.");
		}			
		
		/*** Set local variables ***/
		int progress = Math.min(window_slide,lastsec);
		boolean last_iteration = false;
									
		/*** Get the permission to schedule current slide ***/
		while (eventqueue.getDriverProgress(progress)) {
			
			/*** Schedule the available events ***/
			Event event = eventqueue.contents.peek();
			while (event != null && event.sec <= progress) { 
					
				Event e = eventqueue.contents.poll();
				
				/*** Fill windows with events ***/
				for (Window window : windows2iterate) {
					if (window.relevant(e)) window.events.add(e); 
				}
				/*** Poll an expired window and submit it for execution ***/
				if (!windows2iterate.isEmpty() && windows2iterate.getFirst().expired(e)) {					
					Window window = windows2iterate.poll();
					System.out.println(window.toString());
					execute(window);					
				}
				event = eventqueue.contents.peek();
			}		 
			/*** Update progress ***/
			if (last_iteration) {
				break;
			} else {
				if (progress+window_slide>lastsec) {
					progress = lastsec;
					last_iteration = true;
				} else {
					progress += window_slide;
				}
			}									
		}
		/*** Poll the last windows and submit them for execution ***/
		for (Window window : windows2iterate) {
			System.out.println(window.toString());
			execute(window);			
		}		
		/*** Terminate ***/
		try { transaction_number.await(); } catch (InterruptedException e) { e.printStackTrace(); }
		done.countDown();	
		System.out.println("Scheduler is done.");
	}	
	
	public void execute(Window window) {
		Transaction transaction;
		if (algorithm == 1) {
			transaction = new BaseLine(window.events,output,transaction_number,processingTime,maxMemoryPerWindow);		
		} else {
		if (algorithm == 2) {
			transaction = new NonDynamic(window.events,output,transaction_number,processingTime,maxMemoryPerWindow);
		} else {
		if (algorithm == 3) {
			transaction = new Dynamic(window.events,output,transaction_number,processingTime,maxMemoryPerWindow);
		} else {
			transaction = new Partitioned(window.events,output,transaction_number,processingTime,maxMemoryPerWindow,memory_limit,search_algorithm,windows,window,shared_partitions);
		}}}
		executor.execute(transaction);	
	}
	
	/*public void run() {	
		
		try {		
		*//*** Set local variables ***//*
		int window_end = Math.min(window_length,lastsec);
		int progress = (incremental) ? 0 : window_end;		
		HashSet<TreeSet<Event>> results = new HashSet<TreeSet<Event>>();
		transaction_number = new CountDownLatch(0);
							
		*//*** Get the permission to schedule current second ***//*
		while (progress <= lastsec && eventqueue.getDriverProgress(progress)) {
			
			ArrayList<Event> batch = new ArrayList<Event>();
			if (incremental) progress = Math.min(eventqueue.driverProgress.get(),window_end);
										
			*//*** Schedule the available events ***//*
			Event event = eventqueue.contents.peek();
			while (event != null && event.sec <= progress) { 
					
				Event e = eventqueue.contents.poll();
				//System.out.println(e.toString());
				batch.add(e);
				event = eventqueue.contents.peek();
			}
			*//*** Create a transaction and submit it for execution ***//*
			if (!batch.isEmpty()) {
				transaction_number.await();
				transaction_number = new CountDownLatch(1);
				BaseLine transaction = new BaseLine(batch,startOfSimulation,transaction_number,results,output);				
				executor.execute(transaction);
			}
			*//*** If the stream is over, terminate ***//*
			if (progress == lastsec) {
				transaction_number.await();
				done.countDown();	
				break;
			} else {
				*//*** If window is over, clear the results and update the window end ***//*
				if (progress == window_end) {
					transaction_number.await();
					
					results.clear();
					window_end += window_length;				 
					if (window_end >= lastsec) window_end = lastsec;
				}
				*//*** Update progress ***//*
				progress = (incremental) ? progress+1 : window_end;				
			}			
		}
		} catch (InterruptedException e) { e.printStackTrace(); }
		System.out.println("Scheduler is done.");
	}*/	
}