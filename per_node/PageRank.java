package CS5300.PROJECT2;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class PageRank {
	//static long n = 8;
	static long n = 685230;
	static double d = .85;
	static enum RecordCounters { RESIDUAL };

	public static class FirstMap extends MapReduceBase implements Mapper<LongWritable, Text, LongWritable, Text> {
		public void map(LongWritable key, Text value, OutputCollector<LongWritable, Text> output, Reporter reporter) throws IOException {
			// input: src dst prop
			// output: src dst
			String line = value.toString();
			String[] list = line.trim().split("\\s+");
			//TODO: add filter here
			output.collect(new LongWritable(Long.parseLong(list[0])), new Text(list[1]));
		}
	}
	
	public static class FirstReduce extends MapReduceBase implements Reducer<LongWritable, Text, LongWritable, Text> {
		public void reduce(LongWritable key, Iterator<Text> values, OutputCollector<LongWritable, Text> output, Reporter reporter) throws IOException {
			String nodeList = String.valueOf(1.0/n);
			while (values.hasNext()) {
				nodeList += " "+values.next();
			}
			//System.out.println(nodeList);
			output.collect(key, new Text(nodeList));
		}
	}
	
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, LongWritable, Text> {
		public void map(LongWritable key, Text value, OutputCollector<LongWritable, Text> output, Reporter reporter) throws IOException {
			// input: <u; PR(u), {v|u->v}>
			// output: <u; "L", PR(u), {v|u->v}> <v; PR(u)/deg(u)|u->v>
			String[] list = value.toString().split("\\s+");
			double pr = Double.parseDouble(list[0]);
			int deg = list.length-1;
			String nodeList = "L "+list[0];
			for (int i = 1; i != list.length; i++) {
				nodeList += " "+list[i];
				output.collect(new LongWritable(Long.parseLong(list[i])), new Text(String.valueOf(pr/deg)));
			}
			output.collect(key, new Text(nodeList));
		}
	}
	
	public static class Reduce extends MapReduceBase implements Reducer<LongWritable, Text, LongWritable, Text> {
		public void reduce(LongWritable key, Iterator<Text> values, OutputCollector<LongWritable, Text> output, Reporter reporter) throws IOException {
			// input: <v; "L", PR(v), {w|v->w}> <v, PT(u)/deg(u)|u->v>
			// output: <v; PR(v), {w|v->w}>
			double pr = (1-d)/n;
			double oldPR = 0;
			String nodeList = "";
			while (values.hasNext()) {
				String l = values.next().toString();
				if (l.startsWith("L")) {
					String[] list = l.split("\\s+");
					oldPR = Double.parseDouble(list[1]);
					nodeList = "L";
					for (int i = 2; i != list.length; i++) nodeList += " "+list[i];
				}
				else {
					pr += d*Double.parseDouble(l);
				}
			}
			reporter.getCounter(RecordCounters.RESIDUAL).setValue(reporter.getCounter(RecordCounters.RESIDUAL).getValue()+(long)(Math.abs(pr-oldPR)/pr*1000000));
			if (nodeList == "") {
				nodeList = " ";
			}
			//System.out.print(key.toString()+" "+String.valueOf(pr)+"\n");
			output.collect(key, new Text(String.valueOf(pr)+nodeList.substring(1)));
		}	
	}
	
	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(PageRank.class);
		conf.setJobName("PageRank");

		conf.setOutputKeyClass(LongWritable.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(FirstMap.class);
		//conf.setCombinerClass(FirstReduce.class);
		conf.setReducerClass(FirstReduce.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(SequenceFileOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path("/home/o/workspace/CS5300PROJECT2_WITHERROR/input"));
		FileOutputFormat.setOutputPath(conf, new Path("/home/o/workspace/CS5300PROJECT2_WITHERROR/output0"));

		JobClient.runJob(conf);
		for (int i = 0; i != 5; i++) {
			System.out.println("=====Iteration "+(i+1)+"=====");
			conf = new JobConf(PageRank.class);
			conf.setJobName("PageRank");

			conf.setOutputKeyClass(LongWritable.class);
			conf.setOutputValueClass(Text.class);
			conf.setMapperClass(Map.class);
			//conf.setCombinerClass(FirstReduce.class);
			conf.setReducerClass(Reduce.class);

			conf.setInputFormat(SequenceFileInputFormat.class);
			conf.setOutputFormat(SequenceFileOutputFormat.class);

			FileInputFormat.setInputPaths(conf, new Path("/home/o/workspace/CS5300PROJECT2_WITHERROR/output"+String.valueOf(i)));
			FileOutputFormat.setOutputPath(conf, new Path("/home/o/workspace/CS5300PROJECT2_WITHERROR/output"+String.valueOf(i+1)));
			
			if (i == 4) conf.setOutputFormat(TextOutputFormat.class);

			long errorSum = JobClient.runJob(conf).getCounters().getCounter(RecordCounters.RESIDUAL);
			System.out.println("Iteration "+(i+1)+" residual error: "+(double)errorSum/n/1000000);
		}
	}
}