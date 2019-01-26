import java.io.IOException;
import java.util.StringTokenizer;
//import mutipleInput.Join;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class ProjectTask2 {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private IntWritable Acci = new IntWritable(1);
    private IntWritable Fatal = new IntWritable();
    private IntWritable ppi = new IntWritable();
    private IntWritable ppk = new IntWritable();
    private IntWritable cik = new IntWritable();
    private IntWritable mik = new IntWritable();
     Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringBuffer str = new StringBuffer();
      //StringBuffer str1 = new StringBuffer();
      String line = value.toString();
      String[] itr = line.split(",");
      String[] date;
      date=(itr[0].toString()).split("/");
      int noofAc=0,task22=0,task25=0,task26=0,task27=0,task28=0,j=0;
      /*for (j=0;j<itr.length;j++)
     {
        context.write(new Text(itr[j].toString()),Acci);
        
        
     }*/
     //context.write(new Text(str1.toString()),Acci);
          //context.write(new Text(str.toString()),Acci);
     // Acci.set(1);
      //context.write(new Text(itr1.toString()),Acci);
      //System.out.println(itr1[10]);
    /* for(int i=3;i<11;i++)
      {
        //System.out.println(itr1[i]); 
         
        noofAc=noofAc+Integer.parseInt(itr[i].trim());
        //str1.append(itr1[i]).append(",");  
    }
        Acci.set(noofAc);*/
         context.write(new Text("date-"+itr[0].toString().trim()),Acci);
    for(int i=4;i<11;i=i+2)
      {
        //System.out.println(itr1[i]); 
         
        task22=task22+Integer.parseInt(itr[i].trim());
        //str1.append(itr1[i]).append(",");  
    }
        Fatal.set(task22);
         
        //str1.append(itr[0]);
            
         context.write(new Text("borough-"+itr[1].toString()),Fatal);
         //str1.setLength(0);
         context.write(new Text("zipcode-"+itr[2].toString()),Fatal);
         context.write(new Text("Vehicle type-"+itr[11].toString()),Acci);
         
         for(int i=3;i<11;i++)
      {
         
        if(i==3 || i==5)  {task25=task25+Integer.parseInt(itr[i].toString());}
        if(i==4 || i==6){task26=task26+Integer.parseInt(itr[i].toString());}
        if(i==7 || i==8){task27=task27+Integer.parseInt(itr[i].toString());}
        if(i==9 || i==10){task28=task28+Integer.parseInt(itr[i].toString());}
        
        

        //str1.append(itr1[i]).append(",");  
    }
   
    
    ppi.set(task25);
    ppk.set(task26);
    cik.set(task27);
    mik.set(task28);
    context.write(new Text(("ppinjured_".toString()+date[2].toString())),ppi);
    context.write(new Text(("ppkilled_".toString()+date[2].toString())),ppk);
    context.write(new Text(("cyinkill".toString()+date[2].toString())),cik);
    context.write(new Text(("motinkill".toString()+date[2].toString())),mik);

      }
      }
    
  

  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(ProjectTask2.class);
    job.setMapperClass(TokenizerMapper.class);
    //job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
   // MultipleInputs.addInputPath(job,new Path(args[0]),TextInputFormat.class,TokenizerMapper.class);
    //MultipleInputs.addInputPath(job,new Path(args[1]),TextInputFormat.class,TokenizerMapper.class);
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}