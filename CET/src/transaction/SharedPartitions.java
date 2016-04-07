package transaction;

import java.util.HashMap;
import graph.*;

public class SharedPartitions {

	// Event trends per shared partition
	public HashMap<String,Partition> contents;
				
	public SharedPartitions () {		
		contents = new HashMap<String,Partition>();
	}
	
	public synchronized void add (String partition_id, Partition partition) {		
		contents.put(partition_id, partition);		
		notifyAll();		
	}

	public synchronized Partition get (String partition_id) {		
		try {			
			while (!contents.containsKey(partition_id)) {				
				wait(); 						
			}	
		} catch (InterruptedException e) { e.printStackTrace(); }
		return contents.get(partition_id);		
	}
}
