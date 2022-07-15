import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
public class WordSearchMapper extends Mapper<Long Writable, Text, Text, Text> {
  static String keyword;
  static int pos = 0;
  protected void setup(Context context) throws IOException, InterruptedException{
  Configuration configuration = context.getConfiguration();
  keyword = configuration.get("keyword");
  }
  protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    InputSplit i = context.getInputSplit(); // Get the input split for this map.
    FileSplit f = (FileSplit) i;
    String fileName = f.getPath().getName();
    Integer wordPos;
    pos++;
    if (value.toString().contains(keyword)) {
          wordPos = value.find(keyword);
          context.write(value, new Text (fileName+","+ new IntWritable(pos).
toString()+","+wordPos.toString()));
    }
  }
}
