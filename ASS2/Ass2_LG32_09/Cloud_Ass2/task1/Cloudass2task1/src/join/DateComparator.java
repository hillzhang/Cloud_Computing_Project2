package join;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;


public class DateComparator implements Comparator<String> {

	HashMap<String, String> sort;

	public DateComparator(HashMap<String, String> sort) {
		this.sort = sort;
	}

	public int compare(String arg0, String arg1) {
       SimpleDateFormat format =   new SimpleDateFormat( "yyyy-MM-dd HH:mm:ss" );
		Date date0 = null, date1 = null;

		if (!sort.containsKey(arg0) || !sort.containsKey(arg1)) {
			return 0;
		}
		
		try {
			date0 = format.parse(arg0);
			date1 = format.parse(arg1);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if (date0.after(date1))
		{
			return 1;
		}
		else 
		{
			return -1;
		}
	}
}
	