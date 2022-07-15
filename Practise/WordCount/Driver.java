package com.app; 
import java.io.IOException;
protected void map (LongWritable key, Text value, Context context) throws IOException, InterruptedException {
String [] words=value.toString().split (","); 
        for (String word: words) 
        { 
          context.write (new Text (word), new IntWritable (1));
}}}
