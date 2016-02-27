package transaction;

import java.util.ArrayList;
import java.util.HashMap;
import event.*;

public class SharedPartitions {

	public HashMap<String,HashMap<Integer,ArrayList<EventTrend>>> contents;
				
	public SharedPartitions () {		
		contents = new HashMap<String,HashMap<Integer,ArrayList<EventTrend>>>();
	}
	
	public synchronized void add (String partition_id, HashMap<Integer,ArrayList<EventTrend>> results) {		
		contents.put(partition_id, results);		
		notifyAll();		
	}

	public synchronized boolean isAvailable (String partition_id) {		
		try {			
			while (!contents.containsKey(partition_id)) {				
				wait(); 						
			}	
		} catch (InterruptedException e) { e.printStackTrace(); }
		return true;		
	}
}
