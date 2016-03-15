package optimizer;

import java.util.ArrayDeque;
import event.Window;

public abstract class Partitioner {
	
	 ArrayDeque<Window> windows;
	 
	 public Partitioner (ArrayDeque<Window> w) {
		 windows = w;
	 }
	
	 abstract public Partitioning getPartitioning (Partitioning root, int memeory_limit, int bin_number, int bin_size);	
}
