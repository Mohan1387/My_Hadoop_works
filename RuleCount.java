import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class RuleCount {
	
	
	public static class MapperA extends Mapper <LongWritable, Text,Text, IntWritable>
	{
		public void map(LongWritable key,Text value,Context context)
		{
				
			try {
					String check = value.toString();
					
					final int cnt = 1;
					
					if(check.contains("rule:")){
						
						int indexOfEventIdStart = check.indexOf("rule:");
					    
						int indexOfEventIdEnd = check.indexOf("eventId=", indexOfEventIdStart);
						    
						String rule = check.substring(indexOfEventIdStart, indexOfEventIdEnd);
						
						System.out.println(rule);
						
						Text	k = new Text(rule);
						
						context.write(k, new IntWritable(cnt));
						
					}
					 
		       } catch (Exception e) {
		         e.printStackTrace();
		       }
			
		}
	}
	public static class ReducerA extends Reducer < Text, IntWritable, Text, IntWritable> 
	{
		public void reduce(Text key,Iterable<IntWritable> values,Context context) 
		{	
			try {
				
				int total = 0;
				
				for(IntWritable txt : values){				
					total = total+Integer.parseInt(txt.toString());
				}
				
				context.write(key, new IntWritable(total));
				
			} catch (IOException | InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub

		Configuration conf = new Configuration();

		@SuppressWarnings("deprecation")
		Job job = new Job(conf,"Prob_A");
		job.setJarByClass(RuleCount.class);
		
		job.setMapperClass(MapperA.class);
		job.setReducerClass(ReducerA.class);
		job.setCombinerClass(ReducerA.class);
        	
        job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
	
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
	
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
		
	}

}
