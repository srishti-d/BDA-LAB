import java.io.IOException;
import org.apache.hadoop.io.Text; 
import org.apache.hadoop.mapreduce.Reducer;
public class WordSearch Reducer extends Reducer<Text, Text, Text, Text> {
  protected void reduce(Text key, Text value, Context context) throws IOException, InterruptedException {
    context.write(key, value);
  }
}
