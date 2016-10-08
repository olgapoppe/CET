package event;

public class StockEvent extends Event {
	
	public int sector;
	public String company;
	public double price;
	public int volume;
	public String trtype;
	
	public StockEvent (int sec, int i, double p, int s, String c, int vol, String trt) {
		super(sec,i);
		sector = s;
		company = c;
		price = p;
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
	
	public boolean isCompatible(Event other) {
		if (other instanceof StockEvent) {
			StockEvent o = (StockEvent) other;
			return this.company.equals(o.company) && this.price < o.price && this.sec < o.sec;
		}
		return false;
	}
	
}
