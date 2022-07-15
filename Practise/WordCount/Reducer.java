package com.infosys;

import java.io.IOException;

import org.apache.hadoop.io. IntWritable;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce. Reducer;

public class WordCounterRed extends Reducer<Text, IntWritable, Text, IntWritable>{

@Override

protected void reduce (Text word, Iterable<IntWritable> values, Context context)

throws IOException, InterruptedException {

Integer count = 0;

for (IntWritable val: values) {

count += val.get();
}
context.write(word, new IntWritable(count));
}}
