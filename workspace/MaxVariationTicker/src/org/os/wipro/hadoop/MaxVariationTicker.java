package org.os.wipro.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class MaxVariationTicker {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
Configuration conf = new Configuration();
		
		String[] otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs();
		
		Job job = Job.getInstance(conf, "MaxVariationTicker");
		
		job.setJarByClass(MaxVariationTicker.class);
		job.setMapperClass(MaxVariationTickerMapper.class);
		job.setReducerClass(MaxVariationTickerReducer.class);
		
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FloatWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job,new Path(otherArgs[1]));

		System.out.println(job.waitForCompletion(true));
	}
	
	public static class MaxVariationTickerMapper extends Mapper<LongWritable,Text,Text,FloatWritable>{
		
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
			String line = value.toString();
			String line_split[] = line.split("\t");
			
			Text newKey = new Text(line_split[0]);
			FloatWritable newValue = new FloatWritable(Float.parseFloat(line_split[4])-Float.parseFloat(line_split[5]));
			context.write(newKey, newValue);
		}
	}
	public static class MaxVariationTickerReducer extends Reducer<Text,FloatWritable,Text,FloatWritable>{
		
		public void setup(Context context){
			
		}
		public void reduce(Text key,Iterable<FloatWritable> values,Context context) throws IOException, InterruptedException{
			float max = Float.MIN_VALUE;
			for(FloatWritable value : values){
				if(value.get() > max){
					max = value.get();
				}
			}
			context.write(key, new FloatWritable(max));
		}
	}	
}
