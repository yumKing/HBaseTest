package com.work;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class WordCount {

	public static class MyMap extends Mapper<LongWritable, Text, Text, IntWritable>{

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			String regx = "[\\W0-9]+";
			String[] split = value.toString().split(regx);
			
			for(String sp : split){
				
				if(!sp.equals("")){
					context.write(new Text(sp), new IntWritable(1));
				}
			}
		}
		
	}
	
	public static class MyReduce extends Reducer<Text, IntWritable, Text, IntWritable>{

		@Override
		protected void reduce(Text key, Iterable<IntWritable> value,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
		
			int sum = 0;
			for(IntWritable v : value){
				sum += v.get();
			}
			context.write(key, new IntWritable(sum));
			
		}
		
	}
	
	/**
	 * 配置job工作
	 * @param conf
	 * @param args
	 * @return
	 * @throws Exception
	 */
	public static Job configJob(Configuration conf,String[] args) throws Exception{
		//2、建作业
		Job job = new Job(conf,"countWORD");
		job.setJarByClass(WordCount.class);
		
		//3、输入路径和输入格式
		FileInputFormat.addInputPath(job, new Path(args[0]));
		job.setInputFormatClass(TextInputFormat.class);
		
		//4、mapper
		job.setMapperClass(MyMap.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		//5、reducer
		job.setCombinerClass(MyReduce.class);
		job.setReducerClass(MyReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setNumReduceTasks(1);
		
		//6、输出路径和格式
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setOutputFormatClass(TextOutputFormat.class);
		
		
		return job;
	}
	
	public static void main(String[] args) throws Exception{
		//1、配置
		Configuration conf = new Configuration();
//		Job job = new Job(conf,"countWord");
		String[] oArgs = new GenericOptionsParser(conf,args).getRemainingArgs();
		
		if(oArgs.length != 2 ){
			System.out.println("wrong number of args:"+oArgs.length);
			System.out.println("usage:countword <input> <output>");
			System.exit(-1);
		}
		
		Job job = configJob(conf,oArgs);
		System.exit(job.waitForCompletion(true)?0:1);
	}
}
