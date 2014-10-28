package join;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.TreeMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class JobReducer extends Reducer<Text, Text, Text, Text> {
	Text result = new Text();
	public void reduce(Text key, Iterable<Text> values, 
			Context context
	) throws IOException, InterruptedException {
		Date comdate = null, date = null;
		SimpleDateFormat format =   new SimpleDateFormat( "yyyy-MM-dd HH:mm:ss" );
		String country = null, compacountry = null;
		double duration;
		HashMap<String, String> dateofphoto = new HashMap<String,String>();
		DateComparator bvc = new DateComparator(dateofphoto);
		TreeMap<String, String> sorted = new TreeMap<String, String>(bvc);
		
		HashMap<String, duration> Result = new HashMap<String, duration>();
		
		StringBuffer strBuf = new StringBuffer();

		for (Text text : values){
			String[] val = text.toString().split("\t");
			dateofphoto.put(val[0], val[1]);
		}
		sorted.putAll(dateofphoto);
		
		for(String sdate : sorted.keySet())
		{
			country = dateofphoto.get(sdate);
			if(compacountry == null)
				compacountry = country;
		
			try {
				date = format.parse(sdate);
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			if(comdate == null)
				comdate = date;
			
			if(country.compareTo(compacountry) != 0)
			{
				duration = (date.getTime() - comdate.getTime())*1.0/ (24*60*60*1000);
				
				if (Result.containsKey(compacountry)){
					Result.get(compacountry).add(duration);
				}else{
					Result.put(compacountry, new duration(duration));
				}
				
				compacountry = country;
				comdate = date;
			}
		}
		
		duration = (date.getTime() - comdate.getTime())*1.0/ (24*60*60*1000);
		if(duration==0){
			duration=1.0;
		}
		
		if (Result.containsKey(country)){
			Result.get(country).add(duration);
		}else{
			Result.put(country, new duration(duration));
		}
		
		duration dur = null;
		for(String countrys : Result.keySet())
		{
			dur = Result.get(countrys);
			strBuf.append(countrys + "(" + dur.times + "," + dur.maximum() + "," + dur.minimum() + "," + dur.average() + "," + dur.totalduration() + "), ");
		}
		result.set(strBuf.toString());
		context.write(key, result);
	}
}
