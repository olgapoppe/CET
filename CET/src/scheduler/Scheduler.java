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
	int firstsec;
	int lastsec;
	int window_length;
	int window_slide;
	ArrayDeque<Window> windows;
	int algorithm;
	double memory_limit;
	int cut_number;
	int search_algorithm;
		
	ExecutorService executor;
	
	AtomicInteger drProgress;
	CountDownLatch transaction_number;
	CountDownLatch done;
	
	AtomicLong total_cpu;	
	AtomicInteger total_memory;
	OutputFileGenerator output;
	
	SharedPartitions shared_partitions;
	
	public Scheduler (EventQueue eq, int first, int last, int wl, int ws, int a, double ml, int pn, int sa, 
			ExecutorService exe, AtomicInteger dp, CountDownLatch d, AtomicLong time, AtomicInteger mem, OutputFileGenerator o) {	
		
		eventqueue = eq;
		firstsec = first;
		lastsec = last;
		window_length = wl;
		window_slide = ws;
		windows = new ArrayDeque<Window>();
		algorithm = a;
		memory_limit = ml;
		cut_number = pn;
		search_algorithm = sa;
		
		executor = exe;
		
		drProgress = dp;
		int window_number = (last-first)/ws + 1;
		transaction_number = new CountDownLatch(window_number);
		done = d;
		
		total_cpu = time;
		total_memory = mem;
		output = o;
		
		shared_partitions = new SharedPartitions();
	}
	
	/**
	 * As long as not all events are processed, extract events from the event queue and execute them.
	 */	
	public void run() {	
		
		/*** Create windows ***/	
		ArrayDeque<Window> windows2iterate = new ArrayDeque<Window>();
		int start = firstsec;
		int end = window_length;
		while (start <= lastsec) {
			Window window = new Window(start, end);		
			windows.add(window);
			windows2iterate.add(window);
			start += window_slide;
			end = (start+window_length > lastsec) ? lastsec : (start+window_length); 
			//System.out.println(window.toString() + " is created.");
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
					if (window.events.size() > 1) {
						System.out.println(window.toString());
						execute(window);				
					} else {
						transaction_number.countDown();
					}
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
			if (window.events.size() > 1) {
				System.out.println(window.toString());
				execute(window);			
			} else {
				transaction_number.countDown();
			}
		}		
		/*** Terminate ***/
		try { transaction_number.await(); } catch (InterruptedException e) { e.printStackTrace(); }
		done.countDown();	
		//System.out.println("Scheduler is done.");
	}	
	
	public void execute(Window window) {
		Transaction transaction;
		if (algorithm == 0) {
			transaction = new Sase(window.events,output,transaction_number,total_cpu,total_memory,window);
		} else {
		if (algorithm == 1) {
			transaction = new BaseLine(window.events,output,transaction_number,total_cpu,total_memory,window);		
		} else {
		if (algorithm == 2) {
			transaction = new M_CET(window.events,output,transaction_number,total_cpu,total_memory);
		} else {
		if (algorithm == 3) {
			transaction = new T_CET(window.events,output,transaction_number,total_cpu,total_memory);
		} else {
			transaction = new H_CET(window.events,output,transaction_number,total_cpu,total_memory,memory_limit,cut_number,search_algorithm,windows,window,window_slide,shared_partitions);
		}}}}		
		executor.execute(transaction);	
	}	
}