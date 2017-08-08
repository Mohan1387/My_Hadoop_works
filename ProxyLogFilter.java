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


public class ProxyLogFilter {
	
	public static class MapperA extends Mapper <LongWritable, Text,Text, Text>
	{
		public void map(LongWritable key,Text value,Context context)
		{
				
			try {
					String check = value.toString();
									
						/*To split the String*/
						String[] str = check.split("\"?(\\s+|$)(?=(([^\"]*\"){2})*[^\"]*$)\"?");
				
						if(str.length == 34){
		          Text	k = new Text(str[0]+"\t"+str[1]+"\t"+str[2]); //EventIndex=844358043
		          
		          String val = "";
					
		          for(int i=3;i<str.length;i++){
			        	  
			        	  val = val+str[i]+"\t";
			      }
					
				String fval = val.substring(0, val.length()-1);
					
		         Text v = new Text(fval);
		          
				context.write(k,v);	
						}
		       } catch (Exception e) {
		         e.printStackTrace();
		       }
			
		}
	}
	public static class ReducerA extends Reducer < Text, Text, Text, Text> 
	{
		public void reduce(Text key,Text values,Context context) 
		{	
			try {
				context.write(key, values);
			} catch (IOException | InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub

		Configuration conf = new Configuration();
		conf.set("mapred.job.priority",org.apache.hadoop.mapreduce.JobPriority.LOW.toString());

		@SuppressWarnings("deprecation")
		Job job = new Job(conf,"Prob_A");
		job.setJarByClass(ProxyLogFilter.class);
		
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
		
		
	}

}
