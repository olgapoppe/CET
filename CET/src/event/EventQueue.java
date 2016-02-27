package event;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class EventQueue {
	
	public ConcurrentLinkedQueue<Event> contents;
	public AtomicInteger driverProgress;
				
	public EventQueue (AtomicInteger dp) {		
		contents = new ConcurrentLinkedQueue<Event>();
		driverProgress = dp;		
	}
	
	public synchronized void setDriverProgress (int sec) {
		driverProgress.set(sec);			
		notifyAll();		
	}

	public synchronized boolean getDriverProgress (int sec) {	
		try {			
			while (driverProgress.get() < sec) {				
				wait(); 						
			}	
		} catch (InterruptedException e) { e.printStackTrace(); }
		return true;		
	}
}
