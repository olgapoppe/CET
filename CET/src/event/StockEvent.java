package event;

public class StockEvent extends Event {
	
	public int sector;
	public String company;
	public int volume;
	public String trtype;
	
	public StockEvent (int sec, int i, double p, int s, String c, int vol, String trt) {
		super(sec,i,p);
		sector = s;
		company = c;
		volume = vol;
		trtype = trt;
	}
	
	public static Event parse (String line) {
		
		String[] values = line.split(", ");
		
		int i = Integer.parseInt(values[0]);
		int sec = Integer.parseInt(values[1]);
        int s = Integer.parseInt(values[2]);
        String c = values[3];          	
        double p = Double.parseDouble(values[4]);  
        int v = Integer.parseInt(values[5]);
        String t = values[6]; 
    	    	    	
    	Event event = new StockEvent(sec,i,p,s,c,v,t);    	
    	//System.out.println(event.toString());    	
        return event;
	}

}
