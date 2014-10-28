package join;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ReplicateJoinMapper extends Mapper<Object, Text, Text, Text> {
	private Map <String, String> placeTable = new HashMap<String, String>();
	private Text keyOut = new Text(), valueOut = new Text();
	
	public void setPlaceTable(HashMap<String,String> place){
		placeTable = place;
	}
	
	// get the distributed file and parse it
	public void setup(Context context)
		throws java.io.IOException, InterruptedException{
		
		Path[] cacheFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
		if (cacheFiles != null && cacheFiles.length > 0) {
			String line;
			String[] tokens;
			BufferedReader placeReader = new BufferedReader(new FileReader(cacheFiles[0].toString()));
			try {
				while ((line = placeReader.readLine()) != null) {
					tokens = line.split("\t");
					placeTable.put(tokens[0], tokens[1]);
				}
				System.out.println("size of the place table is: " + placeTable.size());
			} 
			finally {
				placeReader.close();
			}
		}
	}
	
	public void map(Object key, Text value, Context context
	) throws IOException, InterruptedException {
		String[] dataArray = value.toString().split("\t"); //split the data into array
		if (dataArray.length < 6){ // a not complete record with all data
			return; // don't emit anything
		}
		String placeId = dataArray[4];
		String placeName = placeTable.get(placeId);
		if (placeName != null){
			keyOut.set(dataArray[1]);
			valueOut.set(dataArray[3] + "\t" + placeName);
			context.write(keyOut, valueOut);
		}
	}

}
