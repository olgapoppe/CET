package iogenerator;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Random;
import event.*;

public class InputFileGenerator {
	
	static int total_sequence_number; 
	
	public static void main (String[] args) {
		
		int event_number_per_window = 30;
		
		try {
		// Open the output file
		String output_file_name = "CET\\src\\iofiles\\rate" + event_number_per_window + ".txt"; 
		File output_file = new File(output_file_name);
		BufferedWriter output = new BufferedWriter(new FileWriter(output_file));
		
		// Generate the input event stream
		//generate_batches(output);
		generate_windows(output,event_number_per_window);
		
		// Close the file
		output.close();	
		
		} catch (IOException e) { e.printStackTrace(); }
	}
	
	public static void generate_windows (BufferedWriter output, int event_number_per_window) {
		
		// Read input parameters
		int last_sec = 1800;
		int window_length = 600;
		int window_slide = 300;
		int max_comp = 3;
		
		// Local variables
		double window_number = Math.ceil((double)last_sec / (double)window_slide);
				
		// Output the parameters
		System.out.println(
				"Stream length: " + last_sec + " sec" +
				"\nWindow length: " + window_length +
				"\nWindow slide: " + window_slide +
				"\nWindow number: " + window_number +
				"\nEvent number per window: " + event_number_per_window +
				"\nMax compatibility: " + max_comp +
				"\n---------------------");
		
		// Generate windows
		ArrayList<Window> windows = new ArrayList<Window>();
		for (int window_id=0; window_id<window_number; window_id++) {			
			int start = window_id * window_slide;
			int end = start + window_length;
			Window window = new Window(start,end);
			windows.add(window);
		}
		
		// Generate events and keep track of their number per window
		int event_id = 1;
		for (Window window : windows) {			
			ArrayList<Event> events = getEvents(window, event_id, event_number_per_window, max_comp);
			for (Event event : events) {
				try { output.append(event.print2file()); } catch (IOException e) { e.printStackTrace(); }
			}
			event_id += event_number_per_window;
		}	
		// Generate the last event to know the last second in the input stream
		Event last_event = new Event(last_sec,event_id,1);
		try { output.append(last_event.print2file()); } catch (IOException e) { e.printStackTrace(); }
		System.out.println("Average sequence number is " + total_sequence_number/windows.size());
	}	
	
	public static ArrayList<Event> getEvents(Window window, int event_id, int event_number_per_window, int max_comp) {
		
		ArrayList<Event> events = new ArrayList<Event>();		
				
		// First event
		int sec = window.start;
		Event e1 = new Event(sec,event_id,1);
		events.add(e1);
		event_id++;
		
		int event_number = 1;
		int sequence_number = 1;
		Random random = new Random();
		
		while (event_number<event_number_per_window && sec<=window.end) {
			
			// Random time progress 
			sec++;
			// Random event compatibility
			int comp = random.nextInt(max_comp + 1) + 1;
			sequence_number *= comp;
			if (event_number+comp>event_number_per_window) comp = event_number_per_window - event_number;
			// Following events
			for (int i=1; i<=comp; i++) {
				Event e2 = new Event(sec,event_id,1);
				events.add(e2);
				event_id++;
				event_number++;
			}
		}	
		total_sequence_number += sequence_number;
		return events;		
	}
	
	public static void generate_batches (BufferedWriter output) {
		
		// Read input parameters
		int max_time_progress = 150; 
		int max_comp = 3;  
		int last_min = 30;
		int rate_limit = 200;
		
		// Local variables
		Random random = new Random();
		int batch_size = 300; 
		int last_sec = last_min * 60; 
		int comp = 0;	
			
		int event_id = 0;
		int value = 1; 
			
		int sequence_number = 0;
		int max_event_rate = 0;
			
		// Output the parameters
		System.out.println(
				"Stream length: " + last_min + " min" +
				"\nMax time progress: " + max_time_progress +
				"\nMax compatibility: " + max_comp +
				"\nEvent rate limit: " + rate_limit +
				"\n---------------------");
			
		// Generate sequences in batches
		ArrayList<ArrayDeque<Event>> all_events = new ArrayList<ArrayDeque<Event>> ();
			
		for (int iteration_number=0; iteration_number<last_sec/batch_size; iteration_number++) { 
				
			int sec = 0;
			int offset = iteration_number*batch_size;
			int event_number = 0;
			all_events.clear();
						
			System.out.println("Iteration " + iteration_number);
			
			while (sec<=batch_size) {			
			 
				ArrayDeque<Event> events_with_same_value = new ArrayDeque<Event> ();
				int curr_sequence_number = 1;
			
				// First event in a sequence
				sec = random.nextInt(max_time_progress);
				Event e1 = new Event(sec+offset,event_id,value);	
				events_with_same_value.add(e1);				
				event_id++;
				comp = random.nextInt(max_comp + 1) + 1;
				if (comp>1) curr_sequence_number *= comp;
			
				// All following events
				while (comp>0) {
					sec = sec + random.nextInt(max_time_progress) + 1;
					if (sec>batch_size) break;
					for (int i=0; i<comp; i++) {
						Event e2 = new Event(sec+offset,event_id,value);
						events_with_same_value.add(e2);
						event_id++;
					}
					comp = random.nextInt(max_comp + 1) + 1;
					if (comp>1) curr_sequence_number *= comp;
				}	
				all_events.add(events_with_same_value);
				event_number += events_with_same_value.size();
				sequence_number += curr_sequence_number;
				System.out.println(" Value " + value + " graph size " + events_with_same_value.size());
			}
			// Put events in the file in order by time stamp
			int event_rate = write2File(all_events,output,event_number,rate_limit);	
			if (max_event_rate<event_rate) max_event_rate = event_rate;				
		}
		// Output statistics
		System.out.println("---------------------" + 
				"\nSequence number: " + sequence_number +
				"\nEvent number: " + event_id +
				"\nMax event rate: " + max_event_rate);	
	}
	
	public static int write2File(ArrayList<ArrayDeque<Event>> all_events, BufferedWriter output, int event_number, int rate_limit) {
		
		int saved_events = 0;
		int curr_sec = 0;
		int prev_min = 1;
		int event_rate = 0;
		int max_event_rate = 0;
		
		while (saved_events<event_number) {
			int curr_min = curr_sec/60 + 1;
			for (ArrayDeque<Event> events_with_same_value : all_events) {
				while (events_with_same_value.peek()!=null && events_with_same_value.peek().sec == curr_sec) {
					try { 
						Event e = events_with_same_value.poll();
						//if (event_rate <= rate_limit) {
							output.append(e.print2file());
							event_rate++;
						//}						
					} catch (IOException e) { e.printStackTrace(); }
					saved_events++;				
				}
			}
			if (curr_min > prev_min && max_event_rate < event_rate) {
				max_event_rate = event_rate;
				prev_min = curr_min;
				event_rate = 0;
			}
			curr_sec++;				
		}	
		return max_event_rate;
	}
}
