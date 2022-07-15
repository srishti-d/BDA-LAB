import java.io.IOException;
import org.apache.hadoop.conf.Configuration; 
import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.io.LongWritable; 
import org.apache.hadoop.io.NullWritable; 
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper; 
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; 
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class SortStudNames {
    public static class SortMapper extends Mapper<Long Writable, Text, Text, Text> {
       protected void map (Long Writable key, Text value, Context context) throws IOException, InterruptedException {
          String[] token = value.toString().split(","); 
          context.write(new Text(token[1]), new Text(token[0]+"-"+token[1]));
       }
    }
// Here, value is sorted...
    public static class SortReducer extends Reducer<Text, Text, NullWritable, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text details: values) 
            { 
              context.write(NullWritable.get(), details);
            }
        }
    }
public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException{
  Configuration conf= new Configuration();
  Job job= new Job(conf);
  job.setJarByClass(SortEmpNames.class);
  job.setMapperClass(SortMapper.class);
  job.setReducerClass(Sort Reducer.class);
  job.setOutputKeyClass(Text.class);
  job.setOutputValueClass(Text.class);
  FileInputFormat.setInputPaths(job, new Path("/mapreduce/student.csv"));
  FileOutputFormat.setOutputPath(job, new Path("/mapreduce/output/sorted/"));
  System.exit(job.waitForCompletion (true)? 0 : 1);
}}
