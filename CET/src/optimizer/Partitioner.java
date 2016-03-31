package optimizer;

import java.util.ArrayDeque;
import java.util.ArrayList;
import event.*;

public abstract class Partitioner {
	
	 ArrayDeque<Window> windows;
	 
	 public Partitioner (ArrayDeque<Window> w) {
		 windows = w;
	 }
	
	 abstract public Partitioning getPartitioning (ArrayList<Event> batch, double memeory_limit);	
}
