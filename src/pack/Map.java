package pack;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class Map extends Mapper<LongWritable, Text, Text, IntWritable>
	{
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
		StringTokenizer st=new StringTokenizer(value.toString()," ");
		
		while(st.hasMoreTokens())
		{
			String year=st.nextToken();
			String temp=st.nextToken();
			
			int t=Integer.parseInt(temp);
			
			context.write(new Text(year), new IntWritable(t));
		}
		}
	}
	
