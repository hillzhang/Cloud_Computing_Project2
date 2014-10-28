package udf;

import java.util.Date;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.apache.hadoop.hive.ql.exec.UDF;

public class duration extends UDF {

	 private static int MAX_VALUE = 50;
	    private static String comparedColumn[] = new String[MAX_VALUE];
	    private static SimpleDateFormat format = new SimpleDateFormat( "yyyy-MM-dd HH:mm:ss" );
	    private DecimalFormat df = new DecimalFormat("0.0");
	    private static int rowNum = 0;
        private static double duration;
        private static int count;
        
        
	    public double evaluate(Object... args) {
	        String columnValue[] = new String[args.length];
	        for (int i = 0; i < args.length; i++) {
	            columnValue[i] = args[i].toString();
	        }
	        if (rowNum == 0) {
	            for (int i = 0; i < columnValue.length; i++)
	            comparedColumn[i] = columnValue[i];
	            count=0;
	            duration=1.0;
	            rowNum=1;
	            return duration;
	            
	        }

	        if(!columnValue[0].equals(comparedColumn[0])){
	        	for (int i = 0; i < columnValue.length; i++)
	                comparedColumn[i] = columnValue[i];
	        	   duration=1.0;
	        	   count=0;
	        	   return duration;
	        	
	        }
	        else if(!columnValue[2].equals(comparedColumn[2])){
	        	count=1;
	        	Date date1=null;
	        	Date date2=null;
	        	try {
					date1=format.parse(comparedColumn[1]);
					date2=format.parse(columnValue[1]);
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
	        	duration=(date1.getTime()-date2.getTime())*1.0/(24*60*60*1000);
	        	if(duration<0.1)
	        		duration=0.1;
	        	//duration=Double.parseDouble(df.format(duration));
	        	
	        	for (int j = 0; j < columnValue.length; j++) 
	                comparedColumn[j] = columnValue[j];
	        	return duration;
	        }
	        
	        else{
	        	
	        	Date date1=null;
	        	Date date2=null;
	        	try {
					date1=format.parse(comparedColumn[1]);
					date2=format.parse(columnValue[1]);
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
				
				duration=duration+(date1.getTime()-date2.getTime())*1.0/(24*60*60*1000);
				if(duration<0.1)
					duration=0.1;
				//duration=Double.parseDouble(df.format(duration));
	        	for(int j=0;j<columnValue.length;j++)
	        		comparedColumn[j]=columnValue[j];
	        	if(count==0){
	        		if(duration<1.1)
	        			return 0.1;
	        		else return duration-1.0;}
	        	else
	        		return duration;
	        	
	        }
	    
	    }
	
}
