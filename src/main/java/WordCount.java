import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

public class WordCount {
    
    public static class Map extends Mapper<LongWritable,Text,Text,IntWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            super.map(key, value, context);
            
            String line=value.toString();
            StringTokenizer tokens=new StringTokenizer(line);
            while(tokens.hasMoreTokens()){
                Text temp=new Text(tokens.nextToken());
                context.write(temp,new IntWritable(1));
            }
        }
    }
    
    public static class Reduce extends Reducer<Text,IntWritable,Text,IntWritable>{
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            super.reduce(key, values, context);
            int total=0;
            for(IntWritable x:values){
                total+=x.get();
            }
            context.write(key,new IntWritable(total));
        }
    } 

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration(); //adding configuration object, here empty

        // creating job instance with some configurations
        Job job=Job.getInstance(configuration, "WordCount");   
        
        //SETTING CLASSES
        //class of main Class
        job.setJarByClass(WordCount.class); 
    
        //class of mapper
        job.setMapperClass(Map.class);
        
        //class of Reducer and combiner
        job.setReducerClass(Reduce.class);
        job.setCombinerClass(Reduce.class); //yes we want to use combiner
        
        //SETTING IO
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        /*
        * for input classes  key class is not required as we can know using Input Format
        * but for output , 'Outputformat' means in which we way we have to output for example - TextOutput format 
        * means we have to output as a text
        */
        job.setInputFormatClass(TextInputFormat.class);//key=byte offset, value=line
        job.setOutputFormatClass(TextOutputFormat.class); //text ke form me dalldo
        
        //setting file path for input and output
        /*
         we want to run this on hadoop we have to export JAR and then have to run like,
         'hadoop jar WordCount.jar <inputpath> <outputPath>'
        so , args[0] and args[1] will have input and output path respectively,
         
         however for testing we can also run the job on local files by setting 
         input and output path in this project itself and can analyse on the side.
         */

      //        Path inputpath=new Path(args[0]);
      //        Path outputpath=new Path(args[1]);

        /* for debugging only */
        Path inputpath=new Path("./input/words.txt");
        Path outputpath=new Path("./");
        
        FileInputFormat.addInputPath(job,inputpath); 
        FileOutputFormat.setOutputPath(job,outputpath);
         

        //for deleting output path automatically, otherwise before running again we have to delete
        // explicitly , so it do not say file already exists
        outputpath.getFileSystem(configuration).delete(outputpath,true);
        
        System.exit(job.waitForCompletion(true)?0:1);
    }
}