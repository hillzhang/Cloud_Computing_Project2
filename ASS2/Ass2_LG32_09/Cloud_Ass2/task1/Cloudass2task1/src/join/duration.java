package join;

import java.text.DecimalFormat;

public class duration {
	private double duration;
	public int times;
	private double maximum;
	private double minimum;
	private DecimalFormat df = new DecimalFormat("0.0");
	
	public duration() {
		// TODO Auto-generated constructor stub
		times = 0;
		duration = 0.00;
		maximum = 0.00;
		minimum = 0.00;
	}
	
	public duration(double duration) {
		// TODO Auto-generated constructor stub
		times = 1;
		this.duration = duration;
		maximum = duration;
		minimum = duration;
	}
	
	public void add(double duration)
	{
		times = times + 1;
		this.duration += duration;
		if(duration > maximum)
			maximum = duration;
		if(duration < minimum)
			minimum = duration;
	}
	
	public String average()
	{
		double average = duration / times;
		if (average<0.1)
			average=0.1;
		return df.format(average);
	}
	
	public String maximum()
	{
		if(maximum<0.1)
			maximum=0.1;
		return df.format(maximum);
	}
	
	public String minimum()
	{
		if(minimum<0.1)
			minimum=0.1;
		return df.format(minimum);
	}
	
	public String totalduration()
	{
		if(duration<0.1)
			duration=0.1;
		return df.format(duration);
	}


}
