import java.io.IOException;
import java.util.ArrayList;

import javax.print.attribute.standard.JobPriority;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ActiveDirFilter {
	
	public static class MapperA extends Mapper <LongWritable,Text,Text,Text>
	{
		
		public void map(LongWritable key,Text value,Context context){
		
			try {
		        
		         String st = value.toString();
						
						String[] str1 = st.split("\"?(&&|$)(?=(([^\"]*\"){2})*[^\"]*$)\"?");
						
						int test = str1.length;

						if(test > 5){
							
							String datetime =  str1[0];
							String[] datetimea = datetime.split("\\s+");
							String datetimeval = datetimea[0]+" "+datetimea[1]+" "+datetimea[2];
							String DeviceAddr = datetimea[3];
							String EventlogType = datetimea[4].substring(datetimea[4].lastIndexOf("=")+1);
							String  EventIndex = "";
							String  WindowsVersion = "";
							String  WindowsKeyMapFamily = "";
							String  WindowsParserFamily = "";
							String  DetectTime = "";
							String  EventSource = "";
							String  EventID = "";
							String  EventType = "";
							String  EventCategory = "";
							String  User = "";
							String  ComputerName = "";
							String  Description = "";
							String  Message = "";
							String  SubjectSecurityID = "";
							String  SubjectAccountName = "";
							String  SubjectAccountDomain = "";
							String  SubjectLogonID = "";
							String  NewLogonSecurityID = "";
							String  NewLogonAccountName = "";
							String  NewLogonAccountDomain = "";
							String  NewLogonLogonID = "";
							String  LogonType = "";
							String  DetailedAuthenticationInformationLogonProcess = "";
							String  DetailedAuthenticationInformationAuthenticationPackage = "";
							String  NetworkInformationWorkstationName = "";
							String  NewLogonLogonGUID = "";
							String  DetailedAuthenticationInformationTransitedServices = "";
							String  DetailedAuthenticationInformationPackageNameNTLMonly = "";
							String  DetailedAuthenticationInformationKeyLength = "";
							String  ProcessInformationProcessID = "";
							String  ProcessInformationProcessName = "";
							String  NetworkInformationSourceNetworkAddress = "";
							String  NetworkInformationSourcePort = "";
							
							for(int i=1;i<str1.length;i++){
								if( str1[i].contains("EventIndex")){EventIndex = str1[i]; }
								if( str1[i].contains("WindowsVersion")){WindowsVersion = str1[i]; }
								if( str1[i].contains("WindowsKeyMapFamily")){ WindowsKeyMapFamily = str1[i]; }
								if( str1[i].contains("WindowsParserFamily")){ WindowsParserFamily = str1[i]; }
								if( str1[i].contains("DetectTime")){DetectTime = str1[i]; }
								if( str1[i].contains("EventSource")){EventSource = str1[i]; }
								if( str1[i].contains("EventID")){EventID = str1[i]; }
								if( str1[i].contains("EventType")){EventType = str1[i]; }
								if( str1[i].contains("EventCategory")){EventCategory = str1[i]; }
								if( str1[i].contains("User")){User = str1[i]; }
								if( str1[i].contains("ComputerName")){ComputerName = str1[i]; }
								if( str1[i].contains("Description")){Description = str1[i]; }
								if( str1[i].contains("Message")){Message = str1[i]; }
								if( str1[i].contains("Subject:Security ID")){SubjectSecurityID = str1[i]; }
								if( str1[i].contains("Subject:Account Name")){SubjectAccountName = str1[i]; }
								if( str1[i].contains("Subject:Account Domain")){SubjectAccountDomain = str1[i]; }
								if( str1[i].contains("Subject:Logon ID")){SubjectLogonID = str1[i]; }
								if( str1[i].contains("New Logon:Security ID")){NewLogonSecurityID = str1[i]; }
								if( str1[i].contains("New Logon:Account Name")){NewLogonAccountName = str1[i]; }
								if( str1[i].contains("New Logon:Account Domain")){NewLogonAccountDomain = str1[i]; }
								if( str1[i].contains("New Logon:Logon ID")){NewLogonLogonID = str1[i]; }
								if( str1[i].contains("Logon Type")){LogonType = str1[i]; }
								if( str1[i].contains("Detailed Authentication Information:Logon Process")){DetailedAuthenticationInformationLogonProcess = str1[i]; }
								if( str1[i].contains("Detailed Authentication Information:Authentication Package")){DetailedAuthenticationInformationAuthenticationPackage = str1[i]; }
								if( str1[i].contains("Network Information:Workstation Name")){NetworkInformationWorkstationName = str1[i]; }
								if( str1[i].contains("New Logon:Logon GUID")){NewLogonLogonGUID = str1[i]; }
								if( str1[i].contains("Detailed Authentication Information:Transited Services")){DetailedAuthenticationInformationTransitedServices = str1[i]; }
								if( str1[i].contains("Detailed Authentication Information:Package Name (NTLM only)")){DetailedAuthenticationInformationPackageNameNTLMonly = str1[i]; }
								if( str1[i].contains("Detailed Authentication Information:Key Length")){DetailedAuthenticationInformationKeyLength = str1[i]; }
								if( str1[i].contains("Process Information:Process ID")){ProcessInformationProcessID = str1[i]; }
								if( str1[i].contains("Process Information:Process Name")){ProcessInformationProcessName = str1[i]; }
								if( str1[i].contains("Network Information:Source Network Address")){NetworkInformationSourceNetworkAddress = str1[i]; }
								if( str1[i].contains("Network Information:Source Port")){NetworkInformationSourcePort = str1[i]; }
							}
							
						
							ArrayList<String> variablelst = new ArrayList<String>();
							ArrayList<String> fltvariablelst = new ArrayList<String>();
							
							variablelst.add(datetimeval);
							variablelst.add(DeviceAddr);
							variablelst.add(EventlogType);
							variablelst.add(EventIndex);
							variablelst.add(WindowsVersion);
							variablelst.add(WindowsKeyMapFamily);
							variablelst.add(WindowsParserFamily);
							variablelst.add(DetectTime);
							variablelst.add(EventSource);
							variablelst.add(EventID);
							variablelst.add(EventType);
							variablelst.add(EventCategory);
							variablelst.add(User);
							variablelst.add(ComputerName);
							variablelst.add(Description);
							variablelst.add(Message);
							variablelst.add(SubjectSecurityID);
							variablelst.add(SubjectAccountName);
							variablelst.add(SubjectAccountDomain);
							variablelst.add(SubjectLogonID);
							variablelst.add(NewLogonSecurityID);
							variablelst.add(NewLogonAccountName);
							variablelst.add(NewLogonAccountDomain);
							variablelst.add(NewLogonLogonID);
							variablelst.add(LogonType);
							variablelst.add(DetailedAuthenticationInformationLogonProcess);
							variablelst.add(DetailedAuthenticationInformationAuthenticationPackage);
							variablelst.add(NetworkInformationWorkstationName);
							variablelst.add(NewLogonLogonGUID);
							variablelst.add(DetailedAuthenticationInformationTransitedServices);
							variablelst.add(DetailedAuthenticationInformationPackageNameNTLMonly);
							variablelst.add(DetailedAuthenticationInformationKeyLength);
							variablelst.add(ProcessInformationProcessID);
							variablelst.add(ProcessInformationProcessName);
							variablelst.add(NetworkInformationSourceNetworkAddress);
							variablelst.add(NetworkInformationSourcePort);
							
							for(int j=0;j<variablelst.size();j++){
								
								fltvariablelst.add(variablelst.get(j).toString().substring(variablelst.get(j).toString().lastIndexOf("=")+1));
							} 
							
							String fnlresult = "";
							
							for(int m=1;m<fltvariablelst.size();m++){
								
							fnlresult=fnlresult+fltvariablelst.get(m)+"\t";
							
							}
							Text k = new Text(variablelst.get(0));
							Text v = new Text(fnlresult);							
							context.write(k, v);
						}
								        
		       } catch (Exception e) {
		         e.printStackTrace();
		       }
		}
		
	}
	public static class ReducerA extends Reducer <Text,Text,Text,Text>
	{
		
		public void reduce(Text key,Text values,Context context){
			
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
		job.setJarByClass(ActiveDirFilter.class);
		
		job.setMapperClass(MapperA.class);
		//job.setNumReduceTasks(0);
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
