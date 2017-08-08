import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DHCPLogFilter {

	public static class MapperA extends Mapper<LongWritable,Text,Text,Text>{
		
		public void map(LongWritable key,Text value,Context context){
			
			String str = value.toString();
			
			String[] starr = str.split(",");
			
			int test = starr.length;

			if(test >= 7 ){
				String eventId = starr[0].substring(starr[0].lastIndexOf(" ")+1);
				
				//System.out.println(starr[1]+" "+starr[2]+"\t"+eventId+"\t"+starr[4]+"\t"+starr[5]+"\t"+starr[6]);						
				
				
				Text k = new Text(starr[1]+" "+starr[2]);
				Text v = new Text(eventId+"\t"+starr[4]+"\t"+starr[5]+"\t"+starr[6]);
				
				try {
					context.write(k, v);
					
				} catch (IOException | InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
		}
	}
	public static class ReducerA extends Reducer<Text,Text,Text,Text>{
		
		public void reduce(Text key,Text value,Context context){
			
			try {
				context.write(key, value);
				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
	}
	public static void main(String[] args) throws ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		
		Configuration conf = new Configuration();
		conf.set("mapred.job.priority",org.apache.hadoop.mapreduce.JobPriority.LOW.toString());
		
		try {
			@SuppressWarnings("deprecation")
			Job job = new Job(conf,"DHCP Log Filter");
			job.setJarByClass(DHCPLogFilter.class);
			
			job.setMapperClass(MapperA.class);
			job.setReducerClass(ReducerA.class);
			job.setCombinerClass(ReducerA.class);
			
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
		
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
			System.exit(job.waitForCompletion(true) ? 0 : 1);
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		

	}

}
