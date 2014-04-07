package CS5300.PROJECT2;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class PageRank {
	static long n = 685230;
	//static long n = 8;
	static double d = .85;
	static enum RecordCounters { RESIDUAL, BLOCKS, BLOCKITERATES };

	public static class FirstMap extends MapReduceBase implements Mapper<LongWritable, Text, LongWritable, Text> {
		public void map(LongWritable key, Text value, OutputCollector<LongWritable, Text> output, Reporter reporter) throws IOException {
			// input: src dst
			// output: <src: dst>
			String line = value.toString();
			String[] list = line.trim().split("\\s+");
			//TODO: add filter here
			output.collect(new LongWritable(Long.parseLong(list[0])), new Text(list[1]));
		}
	}
	
	public static class FirstReduce extends MapReduceBase implements Reducer<LongWritable, Text, LongWritable, Text> {
		public long blockNum(long key) {
			 long[] b = {10328, 20373, 30629, 40645, 50462, 60841, 70591, 80118, 90497, 100501, 110567, 120945, 130999, 140574,	150953,	161332, 171154, 181514, 191625,	202004,	212383,	222762,	232593,	242878,	252938,	263149,	273210,	283473,	293255, 303043,	313370,	323522,	333883,	343663,	353645,	363929,	374236,	384554, 394929,	404712,	414617,	424747,	434707,	444489,	454285,	464398,	474196,	484050,	493968,	503752,	514131,	524510,	534709,	545088,	555467,	565846,	576225,	586604,	596585,	606367,	616148,	626448,	636240,	646022,	655804,	665666,	675448,	685230};
			 int l = 0, h = b.length-1;
			 int i = (l+h)/2;
			 if (key < b[0]) return 0;
			 if (key > b[b.length-1]) return b.length; 
			 while (h-l > 1) {
				 if (key == b[i]) return i+1;
				 if (key > b[i]) l = i;
				 else h = i;
				 i = (l+h)/2;
			 }
			 return h;
		}
		
		public void reduce(LongWritable key, Iterator<Text> values, OutputCollector<LongWritable, Text> output, Reporter reporter) throws IOException {
			// input: <src: dst>
			// output: <B(src): src, 1, {B(dst), dst|src->dst}>
			String nodeList = String.valueOf(key.get())+" "+String.valueOf(1.0/n);
			while (values.hasNext()) {
				Text v = values.next();	// dst
				nodeList += " "+blockNum(Long.parseLong(v.toString()))+" "+v.toString();
				//System.out.println(nodeList);
			}
			//System.out.println(nodeList);
			output.collect(new LongWritable(blockNum(Long.parseLong(key.toString()))), new Text(nodeList));
		}
	}
	
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, LongWritable, Text> {
		public void map(LongWritable key, Text value, OutputCollector<LongWritable, Text> output, Reporter reporter) throws IOException {
			// input: <B(u): u, PR(u), {B(v), v|u->v}>
			// output: <B(u): u, PR(u), "L", {B(v), v|u->v}>   <B(v): v, B(u), PR(u)/deg(u)|u->v>
			//System.out.println("mapper "+key.toString()+" input: "+value);
			String[] list = value.toString().split("\\s+");
			double pr = Double.parseDouble(list[1]);
			int deg = (list.length-2)/2;
			String nodeList = "L";
			for (int i = 2; i < list.length; i += 2) {
				nodeList += " "+list[i]+" "+list[i+1];
				// output <B(v): v, B(u), PR(u)/deg(u)|u->v>
				//System.out.println("mapper "+key.toString()+" output: <"+list[i]+" : "+list[i+1]+" "+key.toString()+" "+String.valueOf(pr/deg)+">");
				output.collect(new LongWritable(Long.parseLong(list[i])), new Text(list[i+1]+" "+key.toString()+" "+String.valueOf(pr/deg)));
			}
			//System.out.println("mapper "+key.toString()+" output: <"+key+" : "+list[0]+" "+nodeList+">");
			output.collect(key, new Text(list[0]+" "+list[1]+" "+nodeList));
		}
	}
	
	public static class Reduce extends MapReduceBase implements Reducer<LongWritable, Text, LongWritable, Text> {
		
		public void reduce(LongWritable key, Iterator<Text> values, OutputCollector<LongWritable, Text> output, Reporter reporter) throws IOException {
			// input: <B(v): v, PR(v), "L", {B(w), w|v->w}>   <B(v): v, B(u), PR(u)/deg(u)|u->v>
			// output: <B(v): v, PR_new(v), {B(w), w|v->w}>
			TreeMap<Long, Double> nodesInBlock = new TreeMap<Long, Double>();
			Hashtable<String, String> desNodeList = new Hashtable<String, String>();
			TreeMap<Long, ArrayList<String>>edgesInBlock = new TreeMap<Long, ArrayList<String>>();
			Hashtable<String, Double>  nodePR = new Hashtable<String, Double>();
			Hashtable<String, Double>  nextPR = new Hashtable<String, Double>();
			Hashtable<String, Integer> nodeDeg = new Hashtable<String, Integer>();
			Hashtable<String, Double> oldPR = new Hashtable<String, Double>();
			
			while (values.hasNext()) {
				String line = values.next().toString();
				//System.out.println("reducer "+key+" input: "+line);
				String[] l = line.split("\\s+");
				if (!nodesInBlock.containsKey(Long.parseLong(l[0]))) nodesInBlock.put(Long.parseLong(l[0]), (double)0);
				if (l[2].equals("L")) {
					oldPR.put(l[0], Double.parseDouble(l[1]));
					String nodeList = "";
					nodeDeg.put(l[0], (l.length-2)/2);
					for (int i = 3; i < l.length; i += 2) {
						nodeList += " "+l[i]+" "+l[i+1];
						if (l[i].equals(key.toString())) {
							if (!nodesInBlock.containsKey(Long.parseLong(l[i+1]))) nodesInBlock.put(Long.parseLong(l[i+1]), (double)0);
							if (edgesInBlock.containsKey(Long.parseLong(l[i+1]))) {
								ArrayList<String> a = edgesInBlock.get(Long.parseLong(l[i+1]));
								a.add(l[0]);
								edgesInBlock.put(Long.parseLong(l[i+1]), a);
							}
							else {
								ArrayList<String> a = new ArrayList<String>();
								a.add(l[0]);
								edgesInBlock.put(Long.parseLong(l[i+1]), a);
							}
						}
					}
					desNodeList.put(l[0], nodeList);
				}
				else {
					if (nodePR.containsKey(l[0])) nodePR.put(l[0], nodePR.get(l[0])+Double.parseDouble(l[2]));
					else nodePR.put(l[0], Double.parseDouble(l[2]));
					
					if (!l[1].equals(key.toString())) {
						if (nodesInBlock.containsKey(Long.parseLong(l[0]))) nodesInBlock.put(Long.parseLong(l[0]), nodesInBlock.get(Long.parseLong(l[0]))+Double.parseDouble(l[2]));
						else nodesInBlock.put(Long.parseLong(l[0]), Double.parseDouble(l[2]));
					}
				}
			}
			
			// initial PR
			for (Long node : nodesInBlock.keySet()) {
				if (nodePR.containsKey(String.valueOf(node))) nodePR.put(String.valueOf(node), nodePR.get(String.valueOf(node))*d+(1-d)/n);
				else nodePR.put(String.valueOf(node), (1-d)/n);
				
				if (!oldPR.containsKey(String.valueOf(node))) oldPR.put(String.valueOf(node), (double)0);
				
				if (!desNodeList.containsKey(String.valueOf(node))) desNodeList.put(String.valueOf(node), "");
			}
			
			// iteration: max 30 times
			int i;
			for (i = 0; i != 30; i++) {
				// init: 0
				nextPR = new Hashtable<String, Double>();
				
				for (Long node : nodesInBlock.keySet()) {
					nextPR.put(String.valueOf(node), (double)0);
					if (edgesInBlock.containsKey(node)) {
						for (String src : edgesInBlock.get(node)) {
							if (Long.parseLong(src) < node) {
								nextPR.put(String.valueOf(node), nextPR.get(String.valueOf(node))+nextPR.get(src)/nodeDeg.get(src));
							}
							else
								nextPR.put(String.valueOf(node), nextPR.get(String.valueOf(node))+nodePR.get(src)/nodeDeg.get(src));
						}
					}
					nextPR.put(String.valueOf(node), nextPR.get(String.valueOf(node))+nodesInBlock.get(node));
					nextPR.put(String.valueOf(node), d*nextPR.get(String.valueOf(node))+(1-d)/n);
				}
				
				if (i > 0) {
					double error = 0;
					for (Long node : nodesInBlock.keySet()) {
						error += Math.abs((nextPR.get(String.valueOf(node))-nodePR.get(String.valueOf(node))))/nextPR.get(String.valueOf(node));
						nodePR.put(String.valueOf(node), nextPR.get(String.valueOf(node)));
					}
					if (error < 0.001*nodesInBlock.size()) break;
				}
			}
			reporter.getCounter(RecordCounters.BLOCKS).increment(1);
			reporter.getCounter(RecordCounters.BLOCKITERATES).increment(i);
			//System.out.println("Block converged after "+i+" itertions.");

			// output: <B(v): v, PR_new(v), {B(w), w|v->w}>
			for (Long node : nodesInBlock.keySet()) {
				//System.out.println("PR: <"+key.toString()+" : "+node+" "+String.valueOf(nodePR.get(node))+">");
				reporter.getCounter(RecordCounters.RESIDUAL).increment((long)(Math.abs(nodePR.get(String.valueOf(node))-oldPR.get(String.valueOf(node)))/nodePR.get(String.valueOf(node))*1000000));
				output.collect(key, new Text(String.valueOf(node)+" "+String.valueOf(nodePR.get(String.valueOf(node)))+" "+desNodeList.get(String.valueOf(node))));
			}
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

		FileInputFormat.setInputPaths(conf, new Path("/home/o/workspace/CS5300PROJECT2BLOCKED-GS/input"));
		FileOutputFormat.setOutputPath(conf, new Path("/home/o/workspace/CS5300PROJECT2BLOCKED-GS/output0"));
		//FileInputFormat.setInputPaths(conf, new Path("s3n://ot55-cs5300-project2blocked/input"));
		//FileOutputFormat.setOutputPath(conf, new Path("s3n://ot55-cs5300-project2blocked/output0"));

		JobClient.runJob(conf);
		for (int i = 0; i != 7; i++) {
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

			FileInputFormat.setInputPaths(conf, new Path("/home/o/workspace/CS5300PROJECT2BLOCKED-GS/output"+String.valueOf(i)));
			FileOutputFormat.setOutputPath(conf, new Path("/home/o/workspace/CS5300PROJECT2BLOCKED-GS/output"+String.valueOf(i+1)));
			//FileInputFormat.setInputPaths(conf, new Path("s3n://ot55-cs5300-project2blocked/output"+String.valueOf(i)));
			//FileOutputFormat.setOutputPath(conf, new Path("s3n://ot55-cs5300-project2blocked/output"+String.valueOf(i+1)));
			
			if (i == 6) conf.setOutputFormat(TextOutputFormat.class);

			Counters c = JobClient.runJob(conf).getCounters();
			long error = c.getCounter(RecordCounters.RESIDUAL);
			
			System.out.println("Pass "+(i+1)+" residual error: "+(double)error/n/1000000);
			System.out.println("Pass "+(i+1)+" iterations per block: "+(float)c.getCounter(RecordCounters.BLOCKITERATES)/c.getCounter(RecordCounters.BLOCKS));

			if (error < 0.001) break;

		}
	}
}