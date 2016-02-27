package transaction;

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;
import graph.*;

public class SharedPartitions {

	public HashMap<String,Partition> contents;
	public AtomicInteger progress;
				
	public SharedPartitions (AtomicInteger p) {		
		contents = new  HashMap<String,Partition>();
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
