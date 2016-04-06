package transaction;

import java.util.ArrayList;
import java.util.HashMap;
import event.*;

public class SharedPartitions {

	// Event trends per shared partition
	public HashMap<String,ArrayList<EventTrend>> contents;
				
	public SharedPartitions () {		
		contents = new HashMap<String,ArrayList<EventTrend>>();
	}
	
	public synchronized void add (String partition_id, ArrayList<EventTrend> trends) {		
		contents.put(partition_id, trends);		
		notifyAll();		
	}

	public synchronized ArrayList<EventTrend> get (String partition_id) {		
		try {			
			while (!contents.containsKey(partition_id)) {				
				wait(); 						
			}	
		} catch (InterruptedException e) { e.printStackTrace(); }
		return contents.get(partition_id);		
	}
}
