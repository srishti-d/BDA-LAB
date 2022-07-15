package com.app:
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
public class WordCounterMap extends Mapper<LongWritable, Text, Text, IntWritable> { 
  @Override
