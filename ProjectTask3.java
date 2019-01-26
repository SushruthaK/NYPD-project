import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.*;
import java.io.InputStreamReader;
import java.util.Scanner;

public class ProjectTask3 {
    public static int findspaces(String temp) {
		char c = (char)(9);
		int istab = temp.lastIndexOf(c);
		//int forSpace = temp.lastIndexOf(" ");
		return istab;
	}

	public static void main(String[] args) throws IOException{

        
        Scanner in = new Scanner(new FileReader("/home/kasiresa/inputtask/part-r-00000"));
        File file = new File("/home/kasiresa/outputmp.txt");

        FileWriter fw = new FileWriter(file.getAbsoluteFile());
         BufferedWriter bw = new BufferedWriter(fw);
  

       
		String date="", zipcode="",VehicleType="",PpInjured="",PpKilled="",cycinkilled="",motinkilled="",borough="";
		int MaxAcci_date = 0,MaxAcci_borough=0,MaxAcci_zip=0,MaxAcci_PpInjured=0,MaxAcci_VehicleType=0,MaxAcci_PpKilled=0,MaxAcci_cycinkilled=0,MaxAcci_motinkilled=0;
		
			
		while(in.hasNext()) {
	
			String str = in.nextLine();
			if(str.equals("")) continue;
			
			
			if(str.startsWith("date-")) {
				String temp = str.substring(5);
				char c = (char)(9);
				int index = findspaces(temp);
				String first = temp.substring(0, index);
				int val = Integer.parseInt(temp.substring(index+1, temp.length()));
				if(val>MaxAcci_date) {
					MaxAcci_date = val;
					date = first;
				}
			}
			else if(str.startsWith("borough-")) {
				
				String temp = str.substring(8);
				char c = (char)(9);
				int index = findspaces(temp);
				String first = temp.substring(0, index);
				int val = Integer.parseInt(temp.substring(index+1, temp.length()));
				if(val>MaxAcci_borough) {
					MaxAcci_borough = val;
					borough = first;
				}
			}
			else if(str.startsWith("zipcode-")) {
				
				String temp = str.substring(8);
				char c = (char)(9);
				int index = findspaces(temp);
				String first = temp.substring(0, index);
				int val = Integer.parseInt(temp.substring(index+1, temp.length()));
				if(val>MaxAcci_zip) {
					MaxAcci_zip = val;
					zipcode = first;
				}
				
			}
			else if(str.startsWith("Vehicle type-")) {
				
				String temp = str.substring(13);
				char c = (char)(9);
				int index = findspaces(temp);
				String first = temp.substring(0, index);
				int val = Integer.parseInt(temp.substring(index+1, temp.length()));
				if(val>MaxAcci_VehicleType) {
					MaxAcci_VehicleType = val;
					VehicleType = first;
				}
				
			}
			else if(str.startsWith("ppinjured_")) {
				
				String temp = str.substring(10);
				char c = (char)(9);
				int index = findspaces(temp);
				String first = temp.substring(0, index);
				int val = Integer.parseInt(temp.substring(index+1, temp.length()));
				if(val>MaxAcci_PpInjured) {
					MaxAcci_PpInjured = val;
					PpInjured = first;
				}
				
			}
			else if(str.startsWith("ppkilled_")) {
		
				String temp = str.substring(9);
				char c = (char)(9);
				int index = findspaces(temp);
				String first = temp.substring(0, index);
				int val = Integer.parseInt(temp.substring(index+1, temp.length()));
				if(val>MaxAcci_PpKilled) {
					MaxAcci_PpKilled = val;
					PpKilled = first;
				}
			}
			else if(str.startsWith("cyinkill")) {
				String temp = str.substring(8);
				char c = (char)(9);
				int index = findspaces(temp);
				String first = temp.substring(0, index);
				int val = Integer.parseInt(temp.substring(index+1, temp.length()));
				if(val>MaxAcci_cycinkilled) {
					MaxAcci_cycinkilled = val;
					cycinkilled = first;
				}
				
			}
			else if(str.startsWith("motinkill")) {
				String temp = str.substring(9);
				char c = (char)(9);
				int index = findspaces(temp);
				String first = temp.substring(0, index);
				int val = Integer.parseInt(temp.substring(index+1, temp.length()));
				if(val>MaxAcci_motinkilled) {
					MaxAcci_motinkilled = val;
					motinkilled = first;
				}
			}

		}
        bw.write("Maximum number of Accidents are occured on date -" +date+ " - " + MaxAcci_date);
        bw.write("\n");
        bw.write("Borough with maximum count of accident fatality- " + borough+"- " + MaxAcci_borough);
        bw.write("\n");
        bw.write("Zip with maximum count of accident fatality- "+ zipcode+" -" + MaxAcci_zip);
        bw.write("\n");
        bw.write("Which vehicle type is involved in maximum accidents- "+VehicleType+ " -" + MaxAcci_VehicleType);
        bw.write("\n");
        bw.write("Year in which maximum Number Of Persons and Pedestrians Injured- "+PpInjured+ "- " + MaxAcci_PpInjured);
        bw.write("\n");
        bw.write("Year in which maximum Number Of Persons and Pedestrians Killed- "+PpKilled+ "- " + MaxAcci_PpKilled);
        bw.write("\n");
        bw.write("Year in which maximum Number Of Cyclist Injured and Killed (combined) -"+cycinkilled+"-" + MaxAcci_cycinkilled);
        bw.write("\n");
        bw.write("Year in which maximum Number Of Motorist Injured and Killed (combined)- "+ motinkilled+"- " + MaxAcci_motinkilled);
        bw.write("\n");
        
        bw.close();
		
		
	}

}
