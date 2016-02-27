package transaction;

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;
import graph.*;

public class SharedEventTrends {

	public HashMap<Integer,Node> contents;
	public AtomicInteger progress;
				
	public SharedEventTrends (AtomicInteger p) {		
		contents = new  HashMap<Integer,Node>();
		progress = p;		
	}
	
	public synchronized void setProgress (int sec) {		
		progress.set(sec);			
		notifyAll();		
	}

	public synchronized boolean getProgress (int sec) {		
		try {			
			while (progress.get() < sec) {				
				wait(); 						
			}	
		} catch (InterruptedException e) { e.printStackTrace(); }
		return true;		
	}
}
