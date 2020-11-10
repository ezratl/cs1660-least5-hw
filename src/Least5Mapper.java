import java.io.*; 
import java.util.*; 
import org.apache.hadoop.io.Text; 
import org.apache.hadoop.io.LongWritable; 
import org.apache.hadoop.mapreduce.Mapper; 

public class Least5Mapper extends Mapper<Object, 
							Text, Text, LongWritable> { 

	private TreeMap<Long, String> tmap; 

	@Override
	public void setup(Context context) throws IOException, 
									InterruptedException 
	{ 
		tmap = new TreeMap<Long, String>(); 
	} 

	@Override
	public void map(Object key, Text value, 
	Context context) throws IOException, 
					InterruptedException 
	{ 

		// input data format => word	 
		// count (tab seperated) 
		// we split the input data 
		String[] tokens = value.toString().split("\t"); 

		String movie_name = tokens[0]; 
		long wordcount = Long.parseLong(tokens[1]); 

		tmap.put(wordcount, movie_name); 

		// we remove the last key-value 
		// if it's size increases 5 
		if (tmap.size() > 5) 
		{ 
			tmap.remove(tmap.lastKey()); 
		} 
	} 

	@Override
	public void cleanup(Context context) throws IOException, 
									InterruptedException 
	{ 
		for (Map.Entry<Long, String> entry : tmap.entrySet()) 
		{ 

			long count = entry.getKey(); 
			String name = entry.getValue(); 

			context.write(new Text(name), new LongWritable(count)); 
		} 
	} 
} 
