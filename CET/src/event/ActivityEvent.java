package event;

public class ActivityEvent extends Event {
		
	public ActivityEvent (int s, int i, int v) {
		super(s,i,v);
	}
	
	public static Event parse (String line) {
		
		String[] values = line.split(",");
		
		int s = Integer.parseInt(values[1]);
        int i = Integer.parseInt(values[3]);
        int v = Integer.parseInt(values[2]);          	
    	    	    	
    	Event event = new ActivityEvent(s,i,v);    	
    	//System.out.println(event.toString());    	
        return event;
	}

}
