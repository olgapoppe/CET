package event;

public class ActivityEvent extends Event {
	
	int activity;
	int heartRate;
		
	public ActivityEvent (int sec, int pid, int a, int h) {
		super(sec, pid);	
		activity = a;
		heartRate = h;			
	}
	
	/**
	 * Parse the given line and construct an activity report.
	 * @param line	
	 * @return activity report
	 */
	public static ActivityEvent parse (String line) {
		
		String[] values = line.split(",");
		
		int sec = Integer.parseInt(values[1]);
		int pid = Integer.parseInt(values[2]);
		int hr = Integer.parseInt(values[3]);
		int a = Integer.parseInt(values[8]);
		
        ActivityEvent event = new ActivityEvent (sec, pid, a, hr); 
        
    	//System.out.println(event.toString());    	
        return event;
	}	
	
	public boolean isCompatible (Event other) {
		if (other instanceof ActivityEvent) {
			ActivityEvent o = (ActivityEvent) other;
			return this.id == o.id && this.heartRate < o.heartRate && this.activity == o.activity && this.sec < other.sec;
		}
		return false;
	}

}
