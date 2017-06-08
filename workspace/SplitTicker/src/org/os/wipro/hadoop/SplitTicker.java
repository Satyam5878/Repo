package org.os.wipro.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;



public class SplitTicker {

	public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {
		
		Configuration conf = new Configuration();
		
		String[] otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs();
		
		Job job = Job.getInstance(conf, "SplitTicker");
		
		job.setJarByClass(SplitTicker.class);
		job.setMapperClass(SplitTickerMapper.class);
		job.setReducerClass(SplitTickerReducer.class);
		
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setNumReduceTasks(4);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));

		FileOutputFormat.setOutputPath(job,new Path(otherArgs[1]));

		System.out.println(job.waitForCompletion(true));
		
	}
	public static class SplitTickerMapper extends Mapper<LongWritable,Text,Text,Text>{
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
			String line  = value.toString();
			String[] line_split = line.split("\t");
			
			
			Text newKey = new Text(line_split[1]);
			String tmpString = line_split[0]+"\t";
			for(int i=2;i<line_split.length;++i){
				tmpString = tmpString + line_split[i]+"\t";
			}
			Text newValue = new Text(tmpString);
			
			context.write(newKey, newValue );	
		}
	}
	public static class SplitTickerReducer extends Reducer<Text,Text,Text,Text>{
			public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException{
				//do nothing 
				for(Text value: values){
					context.write(key, value);
				}
			}
	}
	
	
	
}
