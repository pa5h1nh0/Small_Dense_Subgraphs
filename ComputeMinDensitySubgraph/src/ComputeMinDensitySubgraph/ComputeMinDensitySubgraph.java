package ComputeMinDensitySubgraph;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedList;

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

public class ComputeMinDensitySubgraph
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
		Configuration minSubgraphConf = new Configuration();
			
		try
		{
			Job minSubgraphJob = new Job(minSubgraphConf,"Finding the closest to threshold subgraph");
			minSubgraphJob.setJarByClass(ComputeMinDensitySubgraph.class);
			minSubgraphJob.setJobName("Finding the closest to threshold subgraph");
				
			// Set output key and value class
			minSubgraphJob.setOutputKeyClass(Text.class);
			minSubgraphJob.setOutputValueClass(Text.class);
				
			// Set Map class
			minSubgraphJob.setMapperClass(ComputeMinDensitySubgraphMapper.class);
			// Set Reducer class
			minSubgraphJob.setReducerClass(ComputeMinDensitySubgraphReducer.class);
				
			// Set Map output key and value classes
			minSubgraphJob.setMapOutputKeyClass(IntWritable.class);
			minSubgraphJob.setMapOutputValueClass(Text.class);
				
			// Set input and output format classes
			minSubgraphJob.setInputFormatClass(TextInputFormat.class);
			minSubgraphJob.setOutputFormatClass(TextOutputFormat.class);
				
			// Set input and output path
			FileInputFormat.addInputPath(minSubgraphJob, new Path(filename_path));
			FileOutputFormat.setOutputPath(minSubgraphJob, new Path(outdir_path));
				
			// Start MapReduce job
			minSubgraphJob.waitForCompletion(true);
		}
		catch (IOException e) { e.printStackTrace(); }
		catch (ClassNotFoundException e) { e.printStackTrace(); }
		catch (InterruptedException e) { e.printStackTrace(); }
	}
	
	public static class ComputeMinDensitySubgraphMapper
		extends Mapper<LongWritable, Text, IntWritable, Text>	//<K_in, V_in, K_out, V_out>
	{
		public void map(LongWritable key, Text edge/*arco "[src]<tab>[dst]"*/, Context context) throws IOException
		{
			if(edge.toString().isEmpty()) return;
			//<key, value> pairs as <1, edge>
			try
			{
				context.write(new IntWritable(1), edge);
			}
			catch (InterruptedException e) { e.printStackTrace(); }
		}
	}

	public static class ComputeMinDensitySubgraphReducer
		extends Reducer<IntWritable, Text, Text, Text>			//<K_in, V_in, K_out, V_out>
	{
		public void reduce(IntWritable key, Iterable<Text> edges, Context context) throws IOException
		{
			final int targetDensity = 4;	//the corresponding rho value
			int edgeCount = 0, nodeCount = 0;
			Hashtable<Integer, LinkedList<Integer>> graph = new Hashtable<>();
			Hashtable<Integer, LinkedList<Integer>> minSubgraph = null;
			
			//FibonacciHeap<Integer> minHeap = new FibonacciHeap<Integer>();
			//Hashtable<Node<Integer>, Integer> heapNodes = new Hashtable<>();
			//Hashtable<Integer, Node<Integer>> fibonacciNodes = new Hashtable<Integer, Node<Integer>>();
			
			int maxIdxNode = 0;
			Iterator<Text> iter = edges.iterator();
			while(iter.hasNext())		//build and populate the graph
			{	
				String[] edge = iter.next().toString().split("\\t");
				int src = Integer.valueOf(edge[0]);
				int dst = Integer.valueOf(edge[1]);
				
				if(src > maxIdxNode) maxIdxNode = src;
				if(dst > maxIdxNode) maxIdxNode = dst;
				
				//add the "src" node to the graph, and update its adjacency list
				if(graph.containsKey(src)) graph.get(src).add(dst);
				else
				{
					LinkedList<Integer> src_neighbors = new LinkedList<>();
					src_neighbors.add(dst);
					
					graph.put(src, src_neighbors);
				}
				
				//add the "dst" node to the graph, and update its adjacency list
				if(graph.containsKey(dst)) graph.get(dst).add(src);
				else
				{
					LinkedList<Integer> dst_neighbors = new LinkedList<>();
					dst_neighbors.add(src);
					
					graph.put(dst, dst_neighbors);
				}
				
				edgeCount++;
			}
			
			nodeCount = graph.size();
			IndexMinPQ<Integer> minHeap = new IndexMinPQ<Integer>(maxIdxNode+1);
			
			//build and populate the minHeap as pairs of <idx=node, key=degree>
			for (int node : graph.keySet())
			{
				minHeap.insert(node, graph.get(node).size());
				System.out.println("node " + node + "\tdegree " + graph.get(node).size());
			}
			
			int minNode, minNodesSub = 100, minEdgeCount = 0;
			while(!minHeap.isEmpty())		//while there are nodes left
			{
				minNode = minHeap.delMin();		//extract the minimum degree node from the heap
				System.out.println("minNode = " + minNode);
				//iterate over the neighbors of the minimum degree node
				for(int neighbor : graph.get(minNode))
				{
					//remove the minimum degree node from the neighbor's adjacency list
					graph.get(neighbor).remove(Integer.valueOf(minNode));
					minHeap.decreaseKey(neighbor, graph.get(neighbor).size());	//update the neighbor node degree
				}
				
				edgeCount-= graph.get(minNode).size();	//update number of edges of the graph
				nodeCount--;							//update number of nodes of the graph
				graph.remove(minNode);					//remove the minimum degree node from the graph structure
				
				if((float)edgeCount/(float)nodeCount >= targetDensity && nodeCount < minNodesSub)
				{
					minNodesSub = nodeCount;
					minEdgeCount = edgeCount;
					minSubgraph = (Hashtable<Integer, LinkedList<Integer>>)graph.clone();
					
					System.out.println("Reducer: minNodesSub = " + minNodesSub);
				}
			}
			
			if(minSubgraph == null) return;
			
			try
			{
				float minDensity = (float)minEdgeCount/(float)minSubgraph.size();
				System.out.println("Reducer: density of the minimum subgraph = " + minDensity);
				context.write(null, new Text(String.valueOf(minDensity)));
			}
			catch (InterruptedException e) { e.printStackTrace(); }
			
			Text output_node = new Text();
			for(int node : minSubgraph.keySet())
			{
				output_node.set(String.valueOf(node));
				try
				{
					System.out.println("Reducer: OUT(null, " + output_node.toString() + ")");
					context.write(null, output_node);
				}
				catch (InterruptedException e) { e.printStackTrace(); }
			}
			
			String hashEdge;
			HashSet<String> edgeSet = new HashSet<String>();
			Text output_edge = new Text();
			for(int src : minSubgraph.keySet())
			{
				for(int dst : minSubgraph.get(Integer.valueOf(src)))
				{
					if(src < dst) hashEdge = src + "\t" + dst;
					else hashEdge = dst + "\t" + src;
					
					if(!edgeSet.contains(hashEdge))
					{
						edgeSet.add(hashEdge);
						output_edge.set(hashEdge);
						try
						{
							System.out.println("Reducer: OUT(null, " + hashEdge + ")");
							context.write(null, output_edge);
						}
						catch (InterruptedException e) { e.printStackTrace(); }
					}
				}
			}
		}
	}
}
