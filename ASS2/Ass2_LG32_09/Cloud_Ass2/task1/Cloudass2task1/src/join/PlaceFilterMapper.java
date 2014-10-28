package join;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class PlaceFilterMapper extends Mapper<Object, Text, Text, Text> {
	private Text placeId= new Text(), placeName = new Text();
	
	
	public void map(Object key, Text value, Context context
	) throws IOException, InterruptedException {
		String[] dataArray = value.toString().split("\t"); //split the data into array
		if (dataArray.length < 7){ // a not complete record with all data
			return; // don't emit anything
		}
		
		if(!dataArray[5].equals("29"))
		{
			String[] place = dataArray[4].split(", ");
			String countryName = place[place.length - 1];
			placeId.set(dataArray[0]);
			placeName.set(countryName);
			context.write(placeId, placeName);
		}
	}
}
