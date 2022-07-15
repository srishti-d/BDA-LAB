import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; 
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat; 
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat; 
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
public class WordSearcher {
  public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
    Configuration conf= new Configuration(); 
    Job job = new Job(conf);
    job.setJarByClass(WordSearcher.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setMapperClass (WordSearchMapper.class);
    job.setReducerClass (WordSearchReducer.class);
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class); 
    job.setNum Reduce Tasks(1);
    job.getConfiguration().set("keyword", "Jack");
    FileInputFormat.setInputPaths(job, new Path("/mapreduce/student.csv"));
    FileOutputFormat.setOutputPath(job, new Path("/mapreduce/output/search")); 
    System.exit(job.waitForCompletion(true)? 0 : 1);
  }
}
