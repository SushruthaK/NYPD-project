import csv  
import os
os.system("hadoop fs -get /user/kasiresa/resultspark /home/kasiresa/")
k="/home/kasiresa/resultspark/"
file = open("/home/kasiresa/outputspark.txt","w") 
file.write("Task 2.1-Date on which maximum number of accidents took place"+"\n")
file.write("\n")
file.write("Task 2.2-Borough with maximum count of accident fatality"+"\n")
file.write("\n")
file.write("Task 2.3-Zip with maximum count of accident fatality"+"\n")
file.write("\n")
file.write("Task 2.4-Which vehicle type is involved in maximum accidents"+"\n")
file.write("\n")
file.write("Task 2.5-Year in which maximum Number Of Persons and Pedestrians Injured"+"\n")
file.write("\n")
file.write("Task 2.6-Year in which maximum Number Of Persons and Pedestrians Killed"+"\n")
file.write("\n")
file.write("Task 2.7-Year in which maximum Number Of Cyclist Injured and Killed (combined)"+"\n")
file.write("\n")
file.write("Task 2.8-Year in which maximum Number Of Motorist Injured and Killed (combined)"+"\n")
file.write("\n")
folders = os.listdir(k)
folders.sort()
for i in folders:
    sub_folder=k+"/"+i
    if(os.path.isdir(sub_folder)):
        files = os.listdir(sub_folder)
        csv_files = filter(lambda x:x[-4:] == '.csv',files)
        if(len(csv_files)>0):
            for j in csv_files:
                with open(sub_folder+"/"+j) as csv_file:
                    csv_reader = csv.reader(csv_file, delimiter=',')
                    for row in csv_reader:
                        #print ("Output From the "+i+"-"+repr(row))
						file.write("Output From the "+i+"-"+repr(row)+"\n")
os.system("hadoop fs -rm -r /user/kasiresa/resultspark")

						
                        