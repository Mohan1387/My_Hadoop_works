import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Scanner;

public class LogNorm {

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub

		try {
	         File ipfile = new File(args[0]);
	         File opfile = new File(args[1]);
	         Scanner scanner = new Scanner(ipfile);
	         
	       
	         FileWriter fout = null;
	         
	         fout = new FileWriter(opfile);
	         
	         while (scanner.hasNextLine()) {
	        	 
				String check = scanner.nextLine();
				
			
				
				//System.out.println(spliter.length);
				
				//if(spliter.length==1){System.out.println(spliter[0]);}
				
				if(check.contains("EventlogType=Security")){
				
				String vname = check.substring(check.lastIndexOf("EventlogType")+1);
				
				
				String EventTime = "Eventtime="+check.substring(0, 15);
				String Ip = "Ip="+check.substring(17, 28);
				
				String newcheck = EventTime+"&&"+Ip+"&&"+vname;
				
				String[] spliter = newcheck.split("&&");
				//System.out.println(spliter.length);
				
				for(int i=0;i<spliter.length;i++){
				
					//if(!spliter[2].contains("EventlogType=Security")){System.out.println(spliter[i]);}
					if(spliter.length!=1){
						
						if(spliter[i].contains("logType=Security"))
						{
							System.out.print("E"+spliter[i]+"\t");
							fout.write("E"+spliter[i]+"\t");
						}
						else{
							System.out.print(spliter[i]+"\t");
							fout.write(spliter[i]+"\t");
						}
						
					}
					
				}
				System.out.println("");
				fout.write("\n"); 
	         }
	         }
	         
	         
	         scanner.close();
	         fout.close();
	       } catch (FileNotFoundException e) {
	         e.printStackTrace();
	       }
		
		
		
	}

}
