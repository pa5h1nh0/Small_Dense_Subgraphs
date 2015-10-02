package GraphPartitioning;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Random;

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

public class GraphPartitioning
{
	//input as "[fileIN] [directoryOUT]"
	public static void main(String[] args)
	{
		if(args.length < 2) return;
		
		String filenameIN = args[0];
		String directoryOUT = args[1];
		
		startJob(filenameIN, directoryOUT);
	}
	
	private static void startJob(String filename_path, String outdir_path)
	{
		Configuration graphpartitioningConf = new Configuration();
		
		try
		{
			Job graphpartitioningJob = new Job(graphpartitioningConf,"Partitioning the original graph");
			graphpartitioningJob.setJarByClass(GraphPartitioning.class);
			graphpartitioningJob.setJobName("Partitioning the original graph");
			
			// Set output key and value class
			graphpartitioningJob.setOutputKeyClass(Text.class);
			graphpartitioningJob.setOutputValueClass(Text.class);
			
			// Set Map class
			graphpartitioningJob.setMapperClass(GraphPartitioningMapper.class);
			// Set Reducer class
			graphpartitioningJob.setReducerClass(GraphPartitioningReducer.class);
			
			// Set Map output key and value classes
			graphpartitioningJob.setMapOutputKeyClass(IntWritable.class);
			graphpartitioningJob.setMapOutputValueClass(Text.class);
			
			// Set input and output format classes
			graphpartitioningJob.setInputFormatClass(TextInputFormat.class);
			graphpartitioningJob.setOutputFormatClass(TextOutputFormat.class);
			
			// Set input and output path
			FileInputFormat.addInputPath(graphpartitioningJob, new Path(filename_path));
			FileOutputFormat.setOutputPath(graphpartitioningJob, new Path(outdir_path));
			
			// Start MapReduce job
			graphpartitioningJob.waitForCompletion(true);
		}
		catch (IOException e) { e.printStackTrace(); }
		catch (ClassNotFoundException e) { e.printStackTrace(); }
		catch (InterruptedException e) { e.printStackTrace(); }
	}
	
	public static class GraphPartitioningMapper
		extends Mapper<LongWritable, Text, IntWritable, Text>	//<K_in, V_in, K_out, V_out>
	{
		//partition the graph, each edge is part of a subgraph
		public void map(LongWritable key, Text record/*arco "[src]<tab>[dst]"*/, Context context) throws IOException
		{
			if(record.toString().isEmpty()) return;	
			
			String[] line = record.toString().split("\\t");
			String edge = line[0] + "\t" + line[1];
			double graphDensity = Double.valueOf(line[2]);
			int partitions = (int)Math.round(graphDensity);
			int partition = new Random().nextInt(partitions);
			
			System.out.println("Mapper: [graphDensity " + graphDensity + "] [partitions " + partitions +
					"] [subgraph " + partition + "] edge - " + edge);
				
			//<key, value> pair as <subgraphID, (edge, density)>
			try
			{
				context.write(new IntWritable(partition), new Text(edge + "\t" + graphDensity));
			}
			catch (InterruptedException e) { e.printStackTrace(); }
		}
	}

	public static class GraphPartitioningReducer
		extends Reducer<IntWritable, Text, Text, Text>			//<K_in, V_in, K_out, V_out>
	{
		public void reduce(IntWritable key, Iterable<Text> edges, Context context) throws IOException
		{
			int graphDensity = 0;
			HashMap<Integer, Integer> hashMap = new HashMap<>();
			
			Iterator iter = edges.iterator();
			Queue<String> queue = new LinkedList<>();
			//while there are edges left
			while(iter.hasNext())
			{
				Text line = (Text) iter.next();
				String edge[] = line.toString().split("\\t");
				queue.add(edge[0] + "\t" + edge[1]);
				
				int src = Integer.valueOf(edge[0]);		//first node of this edge
				int dst = Integer.valueOf(edge[1]);		//second node of this edge
				
				if(hashMap.containsKey(src)) hashMap.put(src, hashMap.get(src)+1);	//update degree of this node
				else hashMap.put(src, 1);
				
				if(hashMap.containsKey(dst)) hashMap.put(dst, hashMap.get(dst)+1);	//update degree of this node
				else hashMap.put(dst, 1);
				
				if(!iter.hasNext()) graphDensity = (int)Math.round(Double.valueOf(edge[2]));
			}
			
			Text output_edge = new Text();
			while(!queue.isEmpty())
			{
				String[] edge_ = queue.poll().split("\\t");
				int src = Integer.valueOf(edge_[0]);
				int dst = Integer.valueOf(edge_[1]);
				
				//CAUTION!	here we are pruning each subgraph
				if(hashMap.get(src) > graphDensity && hashMap.get(dst) > graphDensity)
				{
					/* the output file will be as follows:
					 * ###########################
					 * #     [src]<tab>[dst]     #
					 * # ... [src]<tab>[dst] ... #
					 * # ........................#
					 * ###########################
					 */
					output_edge.set(src + "\t" + dst);
					System.out.println("Reducer: [subgraph " + key.toString() + "] OUT(null, "
							+ output_edge.toString() + ")");
					try
					{
						context.write(null, output_edge);
					}
					catch(InterruptedException e) { e.printStackTrace(); }
				}
			}
		}
	}
}
