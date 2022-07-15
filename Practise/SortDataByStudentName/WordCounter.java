import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io. IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; 
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
public class WordCounter {
public static void main (String[] args) throws IOException,
InterruptedException, ClassNotFoundException (
Job job = new Job ();
job.setJobName ("wordcounter");
job.setJarByClass (WordCounter.class);
job.setMapperClass (WordCounterMap.class); 
job.setReducerClass (WordCounterRed.class);
job.setOutputKeyClass (Text.class);
job.setOutputValueClass (IntWritable.class);
FileInputFormat.addInputPath (job, new Path ("/sample/word.txt"));
FileOutputFormat.setOutputPath (job, new Path ("/sample/wordcount"));
System.exit (job.waitForCompletion (true)? 0: 1);
}}
