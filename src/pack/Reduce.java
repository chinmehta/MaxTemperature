package pack;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reduce extends Reducer<Text, IntWritable, Text, IntWritable>
	{
		public void reduce(Text key,Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
		{
			Integer maxtemp=new Integer(0);
			
			
			for(IntWritable val:values)
			{
				if(val.get()>maxtemp.intValue())
				{
					maxtemp=val.get();
				}
			}
		
			context.write(key, new IntWritable(maxtemp.intValue()));
			
		}
	}
	